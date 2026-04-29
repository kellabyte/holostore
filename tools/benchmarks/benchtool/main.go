package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	targetHoloStore = "holostore"
	targetEtcd      = "etcd"

	opRead  = "read"
	opWrite = "write"

	latencyHistogramHighestMicros = int64(24 * time.Hour / time.Microsecond)
	globalLatencySigfigs          = 3
	bucketLatencySigfigs          = 2
	metricsFlushInterval          = time.Second
	maxTrackedErrorMessages       = 64
	maxErrorSampleBytes           = 240
)

type config struct {
	Target                string        `json:"target"`
	Endpoints             []string      `json:"endpoints"`
	Scenario              string        `json:"scenario"`
	Rate                  int           `json:"rate"`
	Duration              time.Duration `json:"-"`
	DurationRaw           string        `json:"duration"`
	Workers               int           `json:"workers"`
	Connections           int           `json:"connections"`
	WorkerHeadroom        float64       `json:"worker_headroom"`
	WritePct              int           `json:"write_pct"`
	Contention            string        `json:"contention"`
	Keys                  int           `json:"keys"`
	HotKeys               int           `json:"hot_keys"`
	HotPct                int           `json:"hot_pct"`
	ZipfS                 float64       `json:"zipf_s"`
	ZipfV                 float64       `json:"zipf_v"`
	KeyPrefix             string        `json:"key_prefix"`
	ValueBytes            int           `json:"value_bytes"`
	Timeout               time.Duration `json:"-"`
	TimeoutRaw            string        `json:"timeout"`
	OutDir                string        `json:"out_dir"`
	Seed                  int64         `json:"seed"`
	Preload               bool          `json:"preload"`
	PreloadWorkers        int           `json:"preload_workers"`
	PreloadTimeout        time.Duration `json:"-"`
	PreloadTimeoutRaw     string        `json:"preload_timeout"`
	PreloadRetries        int           `json:"preload_retries"`
	QueueCap              int           `json:"queue_cap"`
	Progress              bool          `json:"progress"`
	EtcdSerializableReads bool          `json:"etcd_serializable_reads"`
}

type request struct {
	Seq       int64
	Scheduled time.Time
	Op        string
	KeyIndex  int
}

type result struct {
	Seq            int64
	Op             string
	Scheduled      time.Time
	Started        time.Time
	Completed      time.Time
	Latency        time.Duration
	ServiceLatency time.Duration
	StartLag       time.Duration
	Err            string
}

type bucket struct {
	Completed        int
	OK               int
	Errors           int
	Reads            int
	Writes           int
	Latencies        *hdrhistogram.Histogram
	ServiceLatencies *hdrhistogram.Histogram
	StartLags        *hdrhistogram.Histogram
	ErrorCategories  map[string]*errorCounter
}

type latencySummary struct {
	AvgMs  float64 `json:"avg_ms"`
	MinMs  float64 `json:"min_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P75Ms  float64 `json:"p75_ms"`
	P90Ms  float64 `json:"p90_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	P999Ms float64 `json:"p99_9_ms"`
	MaxMs  float64 `json:"max_ms"`
}

type clientSaturationSummary struct {
	WorkerStarved                    bool    `json:"worker_starved"`
	CompletedWithinScheduledDuration bool    `json:"completed_within_scheduled_duration"`
	DrainSeconds                     float64 `json:"drain_seconds"`
	RecommendedWorkersForServiceP95  int     `json:"recommended_workers_for_service_p95"`
	RecommendedWorkersForServiceP99  int     `json:"recommended_workers_for_service_p99"`
	RecommendedWorkersForServiceP999 int     `json:"recommended_workers_for_service_p99_9"`
}

// runnerRuntimeSummary captures load-generator health counters that affect
// benchmark interpretation but are not database results.
type runnerRuntimeSummary struct {
	HeapAllocBytes            uint64  `json:"heap_alloc_bytes"`
	HeapSysBytes              uint64  `json:"heap_sys_bytes"`
	SysBytes                  uint64  `json:"sys_bytes"`
	NumGC                     uint32  `json:"num_gc"`
	Goroutines                int     `json:"goroutines"`
	RequestQueueCapacity      int     `json:"request_queue_capacity"`
	RequestQueuePeakDepth     int64   `json:"request_queue_peak_depth"`
	ResultQueueCapacity       int     `json:"result_queue_capacity"`
	ResultQueuePeakDepth      int64   `json:"result_queue_peak_depth"`
	SchedulerMaxLagMs         float64 `json:"scheduler_max_lag_ms"`
	SchedulerBlockedCount     int64   `json:"scheduler_blocked_count"`
	SchedulerBlockedSeconds   float64 `json:"scheduler_blocked_seconds"`
	ResultQueueBlockedCount   int64   `json:"result_queue_blocked_count"`
	ResultQueueBlockedSeconds float64 `json:"result_queue_blocked_seconds"`
	MetricsFlushes            int64   `json:"metrics_flushes"`
}

// errorCounter stores exact aggregate counts for a bounded category or tracked
// message fingerprint. First/last seconds are completion-second indexes so the
// report can point to when a failure mode appeared.
type errorCounter struct {
	Category    string `json:"category,omitempty"`
	Message     string `json:"message,omitempty"`
	Count       int64  `json:"count"`
	FirstSecond int    `json:"first_second"`
	LastSecond  int    `json:"last_second"`
	Sample      string `json:"sample,omitempty"`
}

type errorCategorySummary struct {
	Category        string  `json:"category"`
	Count           int64   `json:"count"`
	PercentOfErrors float64 `json:"percent_of_errors"`
	FirstSecond     int     `json:"first_second"`
	LastSecond      int     `json:"last_second"`
	Sample          string  `json:"sample"`
}

type errorMessageSummary struct {
	Category        string  `json:"category"`
	Message         string  `json:"message"`
	Count           int64   `json:"count"`
	PercentOfErrors float64 `json:"percent_of_errors"`
	FirstSecond     int     `json:"first_second"`
	LastSecond      int     `json:"last_second"`
	Sample          string  `json:"sample"`
}

// errorMessageTracker keeps bounded top-message context for summary.json. Exact
// category counts remain unbounded and authoritative; message tracking is
// capped so high-cardinality network errors cannot grow memory with request
// count.
type errorMessageTracker struct {
	entries          map[string]*errorCounter
	untrackedSamples int64
}

type summary struct {
	Target                         string                  `json:"target"`
	Scenario                       string                  `json:"scenario"`
	Endpoints                      []string                `json:"endpoints"`
	Rate                           int                     `json:"rate"`
	Duration                       string                  `json:"duration"`
	Workers                        int                     `json:"workers"`
	Connections                    int                     `json:"connections"`
	WorkerHeadroom                 float64                 `json:"worker_headroom"`
	WritePct                       int                     `json:"write_pct"`
	Contention                     string                  `json:"contention"`
	Keys                           int                     `json:"keys"`
	RequestsScheduled              int64                   `json:"requests_scheduled"`
	Completed                      int64                   `json:"completed"`
	OK                             int64                   `json:"ok"`
	Errors                         int64                   `json:"errors"`
	Reads                          int64                   `json:"reads"`
	Writes                         int64                   `json:"writes"`
	ScheduledThroughputPerSecond   float64                 `json:"scheduled_throughput_per_second"`
	CompletedThroughputPerSecond   float64                 `json:"completed_throughput_per_second"`
	WallClockSeconds               float64                 `json:"wall_clock_seconds"`
	CorrectedLatency               latencySummary          `json:"corrected_latency"`
	ServiceLatency                 latencySummary          `json:"service_latency"`
	StartLag                       latencySummary          `json:"start_lag"`
	ClientSaturation               clientSaturationSummary `json:"client_saturation"`
	RunnerRuntime                  runnerRuntimeSummary    `json:"runner_runtime"`
	ErrorCategories                []errorCategorySummary  `json:"error_categories,omitempty"`
	TopErrorMessages               []errorMessageSummary   `json:"top_error_messages,omitempty"`
	FirstErrorSecond               *int                    `json:"first_error_second,omitempty"`
	LastErrorSecond                *int                    `json:"last_error_second,omitempty"`
	UntrackedErrorMessages         int64                   `json:"untracked_error_messages,omitempty"`
	Warnings                       []string                `json:"warnings,omitempty"`
	CoordinatedOmissionMeasurement string                  `json:"coordinated_omission_measurement"`
	MetricsPath                    string                  `json:"metrics_path"`
	ErrorsPath                     string                  `json:"errors_path"`
	ConfigPath                     string                  `json:"config_path"`
}

type kvClient interface {
	Get(context.Context, string) error
	Put(context.Context, string, string) error
	Close() error
}

// schedulerStats is shared by the scheduler, workers, and collector. All
// fields are atomic because the hot-path goroutines update them without
// coordinating with the result collector.
type schedulerStats struct {
	requestQueuePeakDepth   atomic.Int64
	resultQueuePeakDepth    atomic.Int64
	schedulerMaxLagMicros   atomic.Int64
	schedulerBlockedNanos   atomic.Int64
	schedulerBlockedCount   atomic.Int64
	resultQueueBlockedNanos atomic.Int64
	resultQueueBlockedCount atomic.Int64
	metricsFlushes          atomic.Int64
}

// runStats is the completed in-memory run state. It stores bounded histogram
// aggregates instead of per-operation samples, so memory grows mainly with the
// number of one-second buckets rather than request count.
type runStats struct {
	Buckets              map[int]*bucket
	Global               *bucket
	ErrorMessages        *errorMessageTracker
	ScheduledStart       time.Time
	CompletedAt          time.Time
	TotalRequests        int64
	RequestQueueCapacity int
	ResultQueueCapacity  int
	Scheduler            *schedulerStats
}

// metricsSnapshot caches rendered CSV rows so periodic metrics rewrites do not
// repeatedly summarize old histogram buckets during long runs.
type metricsSnapshot struct {
	rows   map[int][]string
	dirty  map[int]struct{}
	maxSec int
}

func main() {
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		usage(os.Stdout)
		return
	}
	if os.Args[1] != "run" {
		usage(os.Stderr)
		os.Exit(2)
	}
	cfg, err := parseRunArgs(os.Args[2:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: benchtool run [options]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Runs a constant-throughput KV benchmark against HoloStore or etcd.")
	fmt.Fprintln(w, "Latency percentiles are corrected for coordinated omission by measuring")
	fmt.Fprintln(w, "from each operation's scheduled start time to its completion time.")
}

func parseRunArgs(args []string) (config, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	target := fs.String("target", targetHoloStore, "target store: holostore or etcd")
	endpoints := fs.String("endpoints", "", "comma-separated endpoints; defaults are Docker Compose service names")
	scenario := fs.String("scenario", "mixed-uniform", "scenario label included in outputs")
	rate := fs.Int("rate", 1000, "constant offered load in operations per second")
	durationRaw := fs.String("duration", "30s", "benchmark duration, e.g. 30s or 2m")
	workersRaw := fs.String("workers", "", "number of concurrent workers; required")
	connectionsRaw := fs.String("connections", "", "number of target clients/connections; defaults to --workers")
	workerHeadroom := fs.Float64("worker-headroom", 1.0, "headroom multiplier used for worker recommendations")
	writePct := fs.Int("write-pct", 50, "percentage of operations that are writes")
	contention := fs.String("contention", "uniform", "key choice: uniform, single-key, hotspot, or zipf")
	keys := fs.Int("keys", 10000, "keyspace size")
	hotKeys := fs.Int("hot-keys", 1, "number of hot keys used by hotspot contention")
	hotPct := fs.Int("hot-pct", 90, "percentage of traffic sent to hot keys")
	zipfS := fs.Float64("zipf-s", 1.2, "Zipf s parameter; must be > 1.0")
	zipfV := fs.Float64("zipf-v", 1.0, "Zipf v parameter; must be >= 1.0")
	keyPrefix := fs.String("key-prefix", "bench_", "key prefix")
	valueBytes := fs.Int("value-bytes", 128, "write value size in bytes")
	timeoutRaw := fs.String("timeout", "5s", "per-operation timeout")
	outDir := fs.String("out-dir", "/results", "output directory")
	seed := fs.Int64("seed", 1, "random seed")
	preload := fs.Bool("preload", true, "preload keyspace before the timed run")
	preloadWorkers := fs.Int("preload-workers", 0, "preload worker clients; 0 picks a conservative default")
	preloadTimeoutRaw := fs.String("preload-timeout", "30s", "per-operation timeout during preload")
	preloadRetries := fs.Int("preload-retries", 3, "retry attempts for each preload write after the first failure")
	queueCap := fs.Int("queue-cap", 0, "scheduled request queue capacity; 0 picks workers*2")
	progress := fs.Bool("progress", true, "print one-second progress lines")
	etcdSerializableReads := fs.Bool("etcd-serializable-reads", false, "use serializable etcd reads instead of linearizable reads")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	duration, err := time.ParseDuration(*durationRaw)
	if err != nil {
		return config{}, fmt.Errorf("parse --duration: %w", err)
	}
	timeout, err := time.ParseDuration(*timeoutRaw)
	if err != nil {
		return config{}, fmt.Errorf("parse --timeout: %w", err)
	}
	preloadTimeout, err := time.ParseDuration(*preloadTimeoutRaw)
	if err != nil {
		return config{}, fmt.Errorf("parse --preload-timeout: %w", err)
	}
	workers, err := parseRequiredPositiveInt("--workers", *workersRaw)
	if err != nil {
		return config{}, err
	}
	connections, err := resolveConnections(*connectionsRaw, workers)
	if err != nil {
		return config{}, err
	}

	cfg := config{
		Target:                strings.ToLower(strings.TrimSpace(*target)),
		Scenario:              *scenario,
		Rate:                  *rate,
		Duration:              duration,
		DurationRaw:           *durationRaw,
		Workers:               workers,
		Connections:           connections,
		WorkerHeadroom:        *workerHeadroom,
		WritePct:              *writePct,
		Contention:            strings.ToLower(strings.TrimSpace(*contention)),
		Keys:                  *keys,
		HotKeys:               *hotKeys,
		HotPct:                *hotPct,
		ZipfS:                 *zipfS,
		ZipfV:                 *zipfV,
		KeyPrefix:             *keyPrefix,
		ValueBytes:            *valueBytes,
		Timeout:               timeout,
		TimeoutRaw:            *timeoutRaw,
		OutDir:                *outDir,
		Seed:                  *seed,
		Preload:               *preload,
		PreloadWorkers:        *preloadWorkers,
		PreloadTimeout:        preloadTimeout,
		PreloadTimeoutRaw:     *preloadTimeoutRaw,
		PreloadRetries:        *preloadRetries,
		QueueCap:              *queueCap,
		Progress:              *progress,
		EtcdSerializableReads: *etcdSerializableReads,
	}

	if *endpoints == "" {
		cfg.Endpoints = defaultEndpoints(cfg.Target)
	} else {
		cfg.Endpoints = splitCSV(*endpoints)
	}
	return cfg, cfg.validate()
}

func parseRequiredPositiveInt(name string, raw string) (int, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return 0, fmt.Errorf("%s is required; pass an explicit positive integer", name)
	}
	if raw == "auto" {
		return 0, fmt.Errorf("%s must be an explicit positive integer; auto worker selection was removed", name)
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be > 0", name)
	}
	return value, nil
}

func resolveConnections(raw string, workers int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return workers, nil
	}
	return parseRequiredPositiveInt("--connections", raw)
}

func (cfg config) validate() error {
	switch cfg.Target {
	case targetHoloStore, targetEtcd:
	default:
		return fmt.Errorf("--target must be %q or %q", targetHoloStore, targetEtcd)
	}
	if len(cfg.Endpoints) == 0 {
		return errors.New("--endpoints must not be empty")
	}
	if cfg.Rate <= 0 {
		return errors.New("--rate must be > 0")
	}
	if cfg.Duration <= 0 {
		return errors.New("--duration must be > 0")
	}
	if cfg.Workers <= 0 {
		return errors.New("--workers must be > 0")
	}
	if cfg.Connections <= 0 {
		return errors.New("--connections must be > 0")
	}
	if cfg.WorkerHeadroom <= 0 {
		return errors.New("--worker-headroom must be > 0")
	}
	if cfg.WritePct < 0 || cfg.WritePct > 100 {
		return errors.New("--write-pct must be between 0 and 100")
	}
	if cfg.Keys <= 0 {
		return errors.New("--keys must be > 0")
	}
	if cfg.HotKeys <= 0 {
		return errors.New("--hot-keys must be > 0")
	}
	if cfg.HotPct < 0 || cfg.HotPct > 100 {
		return errors.New("--hot-pct must be between 0 and 100")
	}
	if cfg.ValueBytes < 0 {
		return errors.New("--value-bytes must be >= 0")
	}
	if cfg.Timeout <= 0 {
		return errors.New("--timeout must be > 0")
	}
	if cfg.PreloadWorkers < 0 {
		return errors.New("--preload-workers must be >= 0")
	}
	if cfg.PreloadTimeout <= 0 {
		return errors.New("--preload-timeout must be > 0")
	}
	if cfg.PreloadRetries < 0 {
		return errors.New("--preload-retries must be >= 0")
	}
	if cfg.QueueCap < 0 {
		return errors.New("--queue-cap must be >= 0")
	}
	switch cfg.Contention {
	case "uniform", "single-key", "hotspot", "zipf":
	default:
		return errors.New("--contention must be uniform, single-key, hotspot, or zipf")
	}
	if cfg.Contention == "zipf" && (cfg.ZipfS <= 1.0 || cfg.ZipfV < 1.0) {
		return errors.New("--zipf-s must be > 1.0 and --zipf-v must be >= 1.0")
	}
	return nil
}

func defaultEndpoints(target string) []string {
	switch target {
	case targetEtcd:
		return []string{"etcd1:2379", "etcd2:2379", "etcd3:2379"}
	default:
		return []string{"holostore1:6379", "holostore2:6379", "holostore3:6379"}
	}
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func run(cfg config) error {
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	configPath := filepath.Join(cfg.OutDir, "config.json")
	if err := writeJSON(configPath, cfg); err != nil {
		return err
	}

	factory, err := newClientFactory(cfg)
	if err != nil {
		return err
	}

	if cfg.Preload && cfg.WritePct == 100 {
		fmt.Printf("skip preload target=%s keys=%d reason=write-only\n", cfg.Target, cfg.Keys)
	} else if cfg.Preload {
		fmt.Printf(
			"preload target=%s keys=%d workers=%d connections=%d timeout=%s retries=%d\n",
			cfg.Target,
			cfg.Keys,
			preloadWorkerCount(cfg),
			preloadConnectionCount(cfg, preloadWorkerCount(cfg)),
			cfg.PreloadTimeout,
			cfg.PreloadRetries,
		)
		if err := preloadKeys(cfg, factory); err != nil {
			return err
		}
	}

	fmt.Printf(
		"run target=%s scenario=%s endpoints=%s rate=%d/s duration=%s workers=%d connections=%d queue_cap=%d write_pct=%d contention=%s keys=%d histogram=hdr\n",
		cfg.Target,
		cfg.Scenario,
		strings.Join(cfg.Endpoints, ","),
		cfg.Rate,
		cfg.Duration,
		cfg.Workers,
		cfg.Connections,
		requestQueueCapacity(cfg),
		cfg.WritePct,
		cfg.Contention,
		cfg.Keys,
	)

	metricsPath := filepath.Join(cfg.OutDir, "metrics.csv")
	errorsPath := filepath.Join(cfg.OutDir, "errors.csv")
	stats, err := runLoad(cfg, factory, metricsPath, errorsPath)
	if err != nil {
		return err
	}

	summaryPath := filepath.Join(cfg.OutDir, "summary.json")
	runSummary := buildSummary(cfg, stats, metricsPath, errorsPath, configPath)
	if err := writeJSON(summaryPath, runSummary); err != nil {
		return err
	}

	fmt.Printf("wrote metrics: %s\n", metricsPath)
	fmt.Printf("wrote errors: %s\n", errorsPath)
	fmt.Printf("wrote summary: %s\n", summaryPath)
	return nil
}

type clientFactory func(connectionID int) (kvClient, error)

type kvClientPool struct {
	available chan kvClient
	clients   []kvClient
}

func newKVClientPool(connections int, factory clientFactory) (*kvClientPool, error) {
	if connections <= 0 {
		return nil, errors.New("connection pool size must be > 0")
	}
	pool := &kvClientPool{
		available: make(chan kvClient, connections),
		clients:   make([]kvClient, 0, connections),
	}
	for connectionID := 0; connectionID < connections; connectionID++ {
		client, err := factory(connectionID)
		if err != nil {
			_ = pool.Close()
			return nil, fmt.Errorf("create connection client %d: %w", connectionID, err)
		}
		pool.clients = append(pool.clients, client)
		pool.available <- client
	}
	return pool, nil
}

func (p *kvClientPool) Acquire(ctx context.Context) (kvClient, error) {
	select {
	case client := <-p.available:
		return client, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *kvClientPool) Release(client kvClient) {
	p.available <- client
}

func (p *kvClientPool) Close() error {
	var closeErr error
	for _, client := range p.clients {
		if err := client.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func newClientFactory(cfg config) (clientFactory, error) {
	switch cfg.Target {
	case targetHoloStore:
		endpoints := append([]string(nil), cfg.Endpoints...)
		return func(connectionID int) (kvClient, error) {
			return newRESPClient(endpoints[connectionID%len(endpoints)], cfg.Timeout), nil
		}, nil
	case targetEtcd:
		endpoints := normalizeEtcdEndpoints(cfg.Endpoints)
		return func(connectionID int) (kvClient, error) {
			client, err := clientv3.New(clientv3.Config{
				Endpoints:   endpoints,
				DialTimeout: cfg.Timeout,
			})
			if err != nil {
				return nil, err
			}
			return &etcdClient{
				client:            client,
				serializableReads: cfg.EtcdSerializableReads,
			}, nil
		}, nil
	default:
		return nil, fmt.Errorf("unsupported target: %s", cfg.Target)
	}
}

func normalizeEtcdEndpoints(endpoints []string) []string {
	out := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if _, err := url.ParseRequestURI(endpoint); err == nil && strings.Contains(endpoint, "://") {
			out = append(out, endpoint)
		} else {
			out = append(out, "http://"+endpoint)
		}
	}
	return out
}

func preloadKeys(cfg config, factory clientFactory) error {
	workers := preloadWorkerCount(cfg)
	connections := preloadConnectionCount(cfg, workers)
	jobs := make(chan int, workers*2)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var completed atomic.Int64
	var wg sync.WaitGroup

	pool, err := newKVClientPool(connections, factory)
	if err != nil {
		return fmt.Errorf("create preload connection pool: %w", err)
	}
	defer pool.Close()

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case keyIdx, ok := <-jobs:
					if !ok {
						return
					}
					client, err := pool.Acquire(ctx)
					if err != nil {
						return
					}
					attempts, err := preloadPutWithRetries(cfg, client, keyIdx)
					pool.Release(client)
					if err != nil {
						select {
						case errCh <- fmt.Errorf("preload worker %d key %d failed after %d attempts: %w", workerID, keyIdx, attempts, err):
						default:
						}
						stop()
						return
					}
					completed.Add(1)
				}
			}
		}(workerID)
	}

	go func() {
		defer close(jobs)
		for keyIdx := 0; keyIdx < cfg.Keys; keyIdx++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- keyIdx:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	var ticker *time.Ticker
	var ticks <-chan time.Time
	if cfg.Progress {
		ticker = time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		ticks = ticker.C
	}

	for {
		select {
		case err := <-errCh:
			if err != nil {
				stop()
				<-done
				return err
			}
		case <-ticks:
			fmt.Printf("preload progress completed=%d/%d\n", completed.Load(), cfg.Keys)
		case <-done:
			return nil
		}
	}
}

func preloadWorkerCount(cfg config) int {
	if cfg.PreloadWorkers > 0 {
		return cfg.PreloadWorkers
	}
	return min(cfg.Workers, 16)
}

func preloadConnectionCount(cfg config, workers int) int {
	return min(workers, cfg.Connections)
}

func preloadPutWithRetries(cfg config, client kvClient, keyIdx int) (int, error) {
	attempts := cfg.PreloadRetries + 1
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.PreloadTimeout)
		err := client.Put(ctx, keyFor(cfg, keyIdx), valueFor(cfg, int64(keyIdx)))
		cancel()
		if err == nil {
			return attempt + 1, nil
		}
		lastErr = err
		if attempt+1 < attempts {
			time.Sleep(preloadRetryBackoff(attempt))
		}
	}
	return attempts, lastErr
}

func preloadRetryBackoff(attempt int) time.Duration {
	backoff := time.Duration(100*(1<<min(attempt, 4))) * time.Millisecond
	return minDuration(backoff, 2*time.Second)
}

// runLoad schedules the configured constant-throughput workload and records
// corrected latency from each absolute scheduled timestamp. Bounded queues can
// delay dispatch, but the scheduled timestamp is never shifted forward, so
// client backlog remains visible in corrected latency and start-lag metrics.
func runLoad(cfg config, factory clientFactory, metricsPath string, errorsPath string) (*runStats, error) {
	totalRequests := int64(math.Round(float64(cfg.Rate) * cfg.Duration.Seconds()))
	if totalRequests <= 0 {
		return nil, errors.New("computed request count is zero")
	}

	queueCap := requestQueueCapacity(cfg)
	resultQueueCap := resultQueueCapacity(cfg)

	reqCh := make(chan request, queueCap)
	resCh := make(chan result, resultQueueCap)
	scheduler := &schedulerStats{}

	pool, err := newKVClientPool(cfg.Connections, factory)
	if err != nil {
		close(reqCh)
		close(resCh)
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	var wg sync.WaitGroup
	for workerID := 0; workerID < cfg.Workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			builder := newOperationBuilder(cfg)
			for req := range reqCh {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
				var err error
				client, acquireErr := pool.Acquire(ctx)
				started := time.Now()
				if acquireErr != nil {
					err = fmt.Errorf("acquire connection: %w", acquireErr)
				} else {
					key := builder.key(req.KeyIndex)
					if req.Op == opWrite {
						err = client.Put(ctx, key, builder.value(req.Seq))
					} else {
						err = client.Get(ctx, key)
					}
					pool.Release(client)
				}
				cancel()
				completed := time.Now()
				errText := ""
				if err != nil {
					errText = err.Error()
				}
				sendStarted := time.Now()
				resCh <- result{
					Seq:            req.Seq,
					Op:             req.Op,
					Scheduled:      req.Scheduled,
					Started:        started,
					Completed:      completed,
					Latency:        completed.Sub(req.Scheduled),
					ServiceLatency: completed.Sub(started),
					StartLag:       started.Sub(req.Scheduled),
					Err:            errText,
				}
				recordQueueBlock(&scheduler.resultQueueBlockedCount, &scheduler.resultQueueBlockedNanos, time.Since(sendStarted))
				atomicMaxInt64(&scheduler.resultQueuePeakDepth, int64(len(resCh)))
			}
		}(workerID)
	}

	scheduledStart := time.Now().Add(500 * time.Millisecond)
	go produceRequests(cfg, totalRequests, scheduledStart, reqCh, scheduler)

	go func() {
		wg.Wait()
		close(resCh)
	}()

	buckets, global, errorMessages, completedAt, err := collectResults(cfg, scheduledStart, totalRequests, resCh, metricsPath, errorsPath, scheduler)
	if err != nil {
		return nil, err
	}
	return &runStats{
		Buckets:              buckets,
		Global:               global,
		ErrorMessages:        errorMessages,
		ScheduledStart:       scheduledStart,
		CompletedAt:          completedAt,
		TotalRequests:        totalRequests,
		RequestQueueCapacity: queueCap,
		ResultQueueCapacity:  resultQueueCap,
		Scheduler:            scheduler,
	}, nil
}

// produceRequests emits exactly totalRequests schedule slots. If the bounded
// request queue is full, the scheduler blocks, records that pressure, and then
// continues using the original absolute timestamps instead of reducing offered
// load.
func produceRequests(cfg config, totalRequests int64, scheduledStart time.Time, reqCh chan<- request, stats *schedulerStats) {
	defer close(reqCh)
	rng := rand.New(rand.NewSource(cfg.Seed))
	var zipf *rand.Zipf
	if cfg.Contention == "zipf" {
		zipf = rand.NewZipf(rng, cfg.ZipfS, cfg.ZipfV, uint64(cfg.Keys-1))
	}

	for seq := int64(0); seq < totalRequests; seq++ {
		scheduled := scheduledStart.Add(time.Duration(float64(seq) * float64(time.Second) / float64(cfg.Rate)))
		if sleepFor := time.Until(scheduled); sleepFor > 0 {
			time.Sleep(sleepFor)
		}
		if lag := time.Since(scheduled); lag > 0 {
			atomicMaxInt64(&stats.schedulerMaxLagMicros, lag.Microseconds())
		}
		op := opRead
		if rng.Intn(100) < cfg.WritePct {
			op = opWrite
		}
		keyIndex := chooseKey(cfg, rng, zipf)
		sendStarted := time.Now()
		reqCh <- request{
			Seq:       seq,
			Scheduled: scheduled,
			Op:        op,
			KeyIndex:  keyIndex,
		}
		recordQueueBlock(&stats.schedulerBlockedCount, &stats.schedulerBlockedNanos, time.Since(sendStarted))
		atomicMaxInt64(&stats.requestQueuePeakDepth, int64(len(reqCh)))
	}
}

func chooseKey(cfg config, rng *rand.Rand, zipf *rand.Zipf) int {
	switch cfg.Contention {
	case "single-key":
		return 0
	case "hotspot":
		hotKeys := min(cfg.HotKeys, cfg.Keys)
		if rng.Intn(100) < cfg.HotPct || hotKeys == cfg.Keys {
			return rng.Intn(hotKeys)
		}
		return hotKeys + rng.Intn(cfg.Keys-hotKeys)
	case "zipf":
		return int(zipf.Uint64())
	default:
		return rng.Intn(cfg.Keys)
	}
}

// collectResults aggregates worker completions into one-second HDR histogram
// buckets and a global histogram. It periodically rewrites metrics.csv and
// errors.csv atomically so interrupted long runs retain the latest complete
// one-second rows and categorized error counts.
func collectResults(cfg config, scheduledStart time.Time, totalRequests int64, resCh <-chan result, metricsPath string, errorsPath string, stats *schedulerStats) (map[int]*bucket, *bucket, *errorMessageTracker, time.Time, error) {
	buckets := make(map[int]*bucket)
	global := newBucket(globalLatencySigfigs)
	errorMessages := newErrorMessageTracker()
	metrics := newMetricsSnapshot()
	var completedAt time.Time
	var completed int64
	lastPrinted := -1
	ticker := time.NewTicker(metricsFlushInterval)
	defer ticker.Stop()

	for resCh != nil {
		select {
		case res, ok := <-resCh:
			if !ok {
				resCh = nil
				break
			}
			completed++
			if res.Completed.After(completedAt) {
				completedAt = res.Completed
			}
			sec := int(math.Floor(res.Completed.Sub(scheduledStart).Seconds()))
			if sec < 0 {
				sec = 0
			}
			b := buckets[sec]
			if b == nil {
				b = newBucket(bucketLatencySigfigs)
				buckets[sec] = b
			}
			category := classifyError(res.Err)
			recordResult(b, res, sec, category)
			recordResult(global, res, sec, category)
			if category != "" {
				errorMessages.Record(category, res.Err, sec)
			}
			metrics.markDirty(sec)
		case <-ticker.C:
			if cfg.Progress {
				nowSec := int(math.Floor(time.Since(scheduledStart).Seconds())) - 1
				for sec := lastPrinted + 1; sec <= nowSec; sec++ {
					printProgress(sec, buckets[sec], completed, totalRequests)
					lastPrinted = sec
				}
			}
			if err := writeMetricsCSVAtomic(metricsPath, cfg, metrics, buckets); err == nil {
				stats.metricsFlushes.Add(1)
			}
			_ = writeErrorsCSVAtomic(errorsPath, cfg, buckets)
		}
	}

	if cfg.Progress {
		maxSec := maxSecond(buckets)
		for sec := lastPrinted + 1; sec <= maxSec; sec++ {
			printProgress(sec, buckets[sec], completed, totalRequests)
		}
	}
	if err := writeMetricsCSVAtomic(metricsPath, cfg, metrics, buckets); err != nil {
		return buckets, global, errorMessages, completedAt, err
	} else {
		stats.metricsFlushes.Add(1)
	}
	if err := writeErrorsCSVAtomic(errorsPath, cfg, buckets); err != nil {
		return buckets, global, errorMessages, completedAt, err
	}
	return buckets, global, errorMessages, completedAt, nil
}

// newBucket allocates the latency histograms used for either a one-second row
// or the global summary.
func newBucket(sigfigs int) *bucket {
	return &bucket{
		Latencies:        newLatencyHistogram(sigfigs),
		ServiceLatencies: newLatencyHistogram(sigfigs),
		StartLags:        newLatencyHistogram(sigfigs),
		ErrorCategories:  map[string]*errorCounter{},
	}
}

// newLatencyHistogram tracks latency in microseconds with bounded memory and
// enough dynamic range for multi-hour corrected-latency outliers.
func newLatencyHistogram(sigfigs int) *hdrhistogram.Histogram {
	return hdrhistogram.New(1, latencyHistogramHighestMicros, sigfigs)
}

// recordResult updates operation counters, latency histograms, and exact
// per-category error counts for one completed operation.
func recordResult(b *bucket, res result, second int, category string) {
	b.Completed++
	if res.Err == "" {
		b.OK++
	} else {
		b.Errors++
		if category == "" {
			category = classifyError(res.Err)
		}
		recordErrorCounter(b.ErrorCategories, category, res.Err, second)
	}
	if res.Op == opWrite {
		b.Writes++
	} else {
		b.Reads++
	}
	recordDuration(b.Latencies, res.Latency)
	recordDuration(b.ServiceLatencies, res.ServiceLatency)
	recordDuration(b.StartLags, res.StartLag)
}

// recordErrorCounter increments one exact category/message aggregate and keeps
// the first sample so reports can show representative error text without
// retaining every failed operation.
func recordErrorCounter(counters map[string]*errorCounter, key string, sample string, second int) {
	if key == "" {
		return
	}
	counter := counters[key]
	if counter == nil {
		counter = &errorCounter{
			Category:    key,
			FirstSecond: second,
			LastSecond:  second,
			Sample:      truncateErrorSample(sample),
		}
		counters[key] = counter
	}
	counter.Count++
	if second < counter.FirstSecond {
		counter.FirstSecond = second
	}
	if second > counter.LastSecond {
		counter.LastSecond = second
	}
}

// classifyError maps raw client-visible errors into stable categories that are
// cheap to aggregate under failure storms. RESP errors are categorized before
// timeout matching so server-side timeout messages remain distinguishable from
// client-side operation deadlines.
func classifyError(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	lower := strings.ToLower(message)
	switch {
	case strings.HasPrefix(message, "ERR ") || strings.HasPrefix(message, "ERR_"):
		return "resp_error"
	case strings.HasPrefix(lower, "unexpected set response"):
		return "unexpected_response"
	case strings.HasPrefix(lower, "acquire connection:"):
		return "pool_timeout"
	case strings.Contains(lower, "unsupported resp") ||
		strings.Contains(lower, "invalid bulk string") ||
		strings.Contains(lower, "invalid resp") ||
		strings.Contains(lower, "failed to read resp frame"):
		return "protocol_error"
	case strings.Contains(lower, "resourceexhausted") ||
		strings.Contains(lower, "resource exhausted"):
		return "etcd_resource_exhausted"
	case strings.Contains(lower, "rpc error") && strings.Contains(lower, "unavailable"):
		return "etcd_unavailable"
	case strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "deadlineexceeded") ||
		strings.Contains(lower, "i/o timeout") ||
		strings.Contains(lower, "timed out"):
		return "timeout"
	case strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "connection reset") ||
		strings.Contains(lower, "broken pipe") ||
		strings.Contains(lower, "use of closed network connection") ||
		strings.Contains(lower, "no route to host") ||
		lower == "eof" ||
		strings.HasSuffix(lower, ": eof"):
		return "connection_error"
	default:
		return "other"
	}
}

func newErrorMessageTracker() *errorMessageTracker {
	return &errorMessageTracker{
		entries: make(map[string]*errorCounter, maxTrackedErrorMessages),
	}
}

// Record tracks bounded top error-message fingerprints. Volatile network
// address details are folded into stable fingerprints so common timeout and
// connection failures aggregate usefully across sockets.
func (tracker *errorMessageTracker) Record(category string, message string, second int) {
	if tracker == nil || category == "" || message == "" {
		return
	}
	key := errorMessageKey(category, message)
	if counter := tracker.entries[key]; counter != nil {
		counter.Count++
		if second < counter.FirstSecond {
			counter.FirstSecond = second
		}
		if second > counter.LastSecond {
			counter.LastSecond = second
		}
		return
	}
	if len(tracker.entries) >= maxTrackedErrorMessages {
		tracker.untrackedSamples++
		return
	}
	tracker.entries[key] = &errorCounter{
		Category:    category,
		Message:     key,
		Count:       1,
		FirstSecond: second,
		LastSecond:  second,
		Sample:      truncateErrorSample(message),
	}
}

func errorMessageKey(category string, message string) string {
	lower := strings.ToLower(strings.TrimSpace(message))
	switch {
	case strings.Contains(lower, "i/o timeout"):
		return category + ": i/o timeout"
	case strings.Contains(lower, "context deadline exceeded") || strings.Contains(lower, "deadlineexceeded"):
		return category + ": context deadline exceeded"
	case strings.Contains(lower, "connection refused"):
		return category + ": connection refused"
	case strings.Contains(lower, "connection reset"):
		return category + ": connection reset"
	case strings.Contains(lower, "broken pipe"):
		return category + ": broken pipe"
	case lower == "eof" || strings.HasSuffix(lower, ": eof"):
		return category + ": eof"
	default:
		return truncateErrorSample(strings.TrimSpace(message))
	}
}

func truncateErrorSample(message string) string {
	message = strings.Join(strings.Fields(strings.TrimSpace(message)), " ")
	if len(message) <= maxErrorSampleBytes {
		return message
	}
	if maxErrorSampleBytes <= 3 {
		return message[:maxErrorSampleBytes]
	}
	return message[:maxErrorSampleBytes-3] + "..."
}

// recordDuration stores a duration in microseconds, clamping only impossible
// negative values and extreme values beyond the configured histogram range.
func recordDuration(histogram *hdrhistogram.Histogram, duration time.Duration) {
	micros := duration.Microseconds()
	if micros < 0 {
		micros = 0
	}
	if micros > latencyHistogramHighestMicros {
		micros = latencyHistogramHighestMicros
	}
	_ = histogram.RecordValue(micros)
}

func printProgress(sec int, b *bucket, completed int64, total int64) {
	if b == nil {
		fmt.Printf("sec=%d completed=0 throughput=0/s errors=0 avg=0.000ms min=0.000ms p50=0.000ms p75=0.000ms p90=0.000ms p95=0.000ms p99=0.000ms p99.9=0.000ms max=0.000ms service_p50=0.000ms start_lag_p95=0.000ms total=%d/%d\n", sec, completed, total)
		return
	}
	stats := summarizeHistogram(b.Latencies)
	service := summarizeHistogram(b.ServiceLatencies)
	startLag := summarizeHistogram(b.StartLags)
	fmt.Printf(
		"sec=%d completed=%d throughput=%d/s errors=%d avg=%.3fms min=%.3fms p50=%.3fms p75=%.3fms p90=%.3fms p95=%.3fms p99=%.3fms p99.9=%.3fms max=%.3fms service_p50=%.3fms start_lag_p95=%.3fms total=%d/%d\n",
		sec,
		b.Completed,
		b.Completed,
		b.Errors,
		stats.AvgMs,
		stats.MinMs,
		stats.P50Ms,
		stats.P75Ms,
		stats.P90Ms,
		stats.P95Ms,
		stats.P99Ms,
		stats.P999Ms,
		stats.MaxMs,
		service.P50Ms,
		startLag.P95Ms,
		completed,
		total,
	)
}

func newMetricsSnapshot() *metricsSnapshot {
	return &metricsSnapshot{
		rows:   map[int][]string{},
		dirty:  map[int]struct{}{},
		maxSec: -1,
	}
}

func (s *metricsSnapshot) markDirty(sec int) {
	s.dirty[sec] = struct{}{}
	if sec > s.maxSec {
		s.maxSec = sec
	}
}

func (s *metricsSnapshot) refresh(cfg config, buckets map[int]*bucket) {
	for sec := range s.dirty {
		b := buckets[sec]
		if b == nil {
			b = newBucket(bucketLatencySigfigs)
		}
		s.rows[sec] = metricsRow(cfg, sec, b)
	}
	clear(s.dirty)
}

// writeMetricsCSVAtomic writes metrics through a same-directory temporary file
// and rename so readers either see the previous complete snapshot or the new
// complete snapshot.
func writeMetricsCSVAtomic(path string, cfg config, snapshot *metricsSnapshot, buckets map[int]*bucket) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create metrics directory: %w", err)
	}
	snapshot.refresh(cfg, buckets)
	tempPath := path + ".tmp"
	if err := writeMetricsCSV(tempPath, cfg, snapshot); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("replace metrics csv: %w", err)
	}
	return nil
}

func writeMetricsCSV(path string, cfg config, snapshot *metricsSnapshot) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create metrics csv: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(metricsHeader()); err != nil {
		return fmt.Errorf("write metrics header: %w", err)
	}

	for sec := 0; sec <= snapshot.maxSec; sec++ {
		row := snapshot.rows[sec]
		if row == nil {
			row = emptyMetricsRow(cfg, sec)
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("write metrics row: %w", err)
		}
	}
	return writer.Error()
}

// writeErrorsCSVAtomic writes exact per-second error category counts. It uses a
// separate artifact from metrics.csv so normal progress rows stay compact while
// failure diagnostics remain available for plotting and reports.
func writeErrorsCSVAtomic(path string, cfg config, buckets map[int]*bucket) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create errors directory: %w", err)
	}
	tempPath := path + ".tmp"
	if err := writeErrorsCSV(tempPath, cfg, buckets); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("replace errors csv: %w", err)
	}
	return nil
}

func writeErrorsCSV(path string, cfg config, buckets map[int]*bucket) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create errors csv: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"target", "scenario", "second", "category", "count", "sample"}); err != nil {
		return fmt.Errorf("write errors header: %w", err)
	}

	for _, sec := range sortedSeconds(buckets) {
		bucket := buckets[sec]
		if bucket == nil || len(bucket.ErrorCategories) == 0 {
			continue
		}
		for _, counter := range sortedErrorCounters(bucket.ErrorCategories) {
			row := []string{
				cfg.Target,
				cfg.Scenario,
				strconv.Itoa(sec),
				counter.Category,
				strconv.FormatInt(counter.Count, 10),
				counter.Sample,
			}
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("write errors row: %w", err)
			}
		}
	}
	return writer.Error()
}

func metricsHeader() []string {
	return []string{
		"target",
		"scenario",
		"second",
		"completed",
		"ok",
		"errors",
		"reads",
		"writes",
		"throughput",
		"avg_ms",
		"min_ms",
		"p50_ms",
		"p75_ms",
		"p90_ms",
		"p95_ms",
		"p99_ms",
		"p99_9_ms",
		"max_ms",
		"service_avg_ms",
		"service_min_ms",
		"service_p50_ms",
		"service_p75_ms",
		"service_p90_ms",
		"service_p95_ms",
		"service_p99_ms",
		"service_p99_9_ms",
		"service_max_ms",
		"start_lag_avg_ms",
		"start_lag_p50_ms",
		"start_lag_p95_ms",
		"start_lag_p99_ms",
		"start_lag_max_ms",
	}
}

func emptyMetricsRow(cfg config, sec int) []string {
	return metricsRow(cfg, sec, newBucket(bucketLatencySigfigs))
}

func metricsRow(cfg config, sec int, b *bucket) []string {
	corrected := summarizeHistogram(b.Latencies)
	service := summarizeHistogram(b.ServiceLatencies)
	startLag := summarizeHistogram(b.StartLags)
	return []string{
		cfg.Target,
		cfg.Scenario,
		strconv.Itoa(sec),
		strconv.Itoa(b.Completed),
		strconv.Itoa(b.OK),
		strconv.Itoa(b.Errors),
		strconv.Itoa(b.Reads),
		strconv.Itoa(b.Writes),
		formatFloat(float64(b.Completed)),
		formatFloat(corrected.AvgMs),
		formatFloat(corrected.MinMs),
		formatFloat(corrected.P50Ms),
		formatFloat(corrected.P75Ms),
		formatFloat(corrected.P90Ms),
		formatFloat(corrected.P95Ms),
		formatFloat(corrected.P99Ms),
		formatFloat(corrected.P999Ms),
		formatFloat(corrected.MaxMs),
		formatFloat(service.AvgMs),
		formatFloat(service.MinMs),
		formatFloat(service.P50Ms),
		formatFloat(service.P75Ms),
		formatFloat(service.P90Ms),
		formatFloat(service.P95Ms),
		formatFloat(service.P99Ms),
		formatFloat(service.P999Ms),
		formatFloat(service.MaxMs),
		formatFloat(startLag.AvgMs),
		formatFloat(startLag.P50Ms),
		formatFloat(startLag.P95Ms),
		formatFloat(startLag.P99Ms),
		formatFloat(startLag.MaxMs),
	}
}

func buildSummary(cfg config, stats *runStats, metricsPath string, errorsPath string, configPath string) summary {
	global := stats.Global
	completed := int64(global.Completed)
	ok := int64(global.OK)
	errs := int64(global.Errors)
	reads := int64(global.Reads)
	writes := int64(global.Writes)
	totalRequests := stats.TotalRequests
	wallSeconds := cfg.Duration.Seconds()
	if !stats.ScheduledStart.IsZero() && !stats.CompletedAt.IsZero() {
		// This includes any drain time when the target cannot keep up with the offered load.
		wallSeconds = math.Max(cfg.Duration.Seconds(), stats.CompletedAt.Sub(stats.ScheduledStart).Seconds())
	}
	correctedLatency := summarizeHistogram(global.Latencies)
	serviceLatency := summarizeHistogram(global.ServiceLatencies)
	startLag := summarizeHistogram(global.StartLags)
	clientSaturation := buildClientSaturationSummary(cfg, serviceLatency, startLag, wallSeconds)
	runtimeSummary := buildRunnerRuntimeSummary(stats)
	warnings := buildWarnings(cfg, correctedLatency, serviceLatency, startLag, clientSaturation, runtimeSummary)
	errorCategories := buildErrorCategorySummaries(global, errs)
	topErrorMessages := buildTopErrorMessageSummaries(stats.ErrorMessages, errs)
	firstErrorSecond, lastErrorSecond := errorSecondRange(errorCategories)
	return summary{
		Target:                         cfg.Target,
		Scenario:                       cfg.Scenario,
		Endpoints:                      cfg.Endpoints,
		Rate:                           cfg.Rate,
		Duration:                       cfg.DurationRaw,
		Workers:                        cfg.Workers,
		Connections:                    cfg.Connections,
		WorkerHeadroom:                 cfg.WorkerHeadroom,
		WritePct:                       cfg.WritePct,
		Contention:                     cfg.Contention,
		Keys:                           cfg.Keys,
		RequestsScheduled:              totalRequests,
		Completed:                      completed,
		OK:                             ok,
		Errors:                         errs,
		Reads:                          reads,
		Writes:                         writes,
		ScheduledThroughputPerSecond:   float64(totalRequests) / cfg.Duration.Seconds(),
		CompletedThroughputPerSecond:   safeDiv(float64(completed), wallSeconds),
		WallClockSeconds:               wallSeconds,
		CorrectedLatency:               correctedLatency,
		ServiceLatency:                 serviceLatency,
		StartLag:                       startLag,
		ClientSaturation:               clientSaturation,
		RunnerRuntime:                  runtimeSummary,
		ErrorCategories:                errorCategories,
		TopErrorMessages:               topErrorMessages,
		FirstErrorSecond:               firstErrorSecond,
		LastErrorSecond:                lastErrorSecond,
		UntrackedErrorMessages:         untrackedErrorMessages(stats.ErrorMessages),
		Warnings:                       warnings,
		CoordinatedOmissionMeasurement: "corrected latency is measured from scheduled start time to completion time; service latency is measured from actual worker start time to completion time",
		MetricsPath:                    metricsPath,
		ErrorsPath:                     errorsPath,
		ConfigPath:                     configPath,
	}
}

func buildErrorCategorySummaries(global *bucket, totalErrors int64) []errorCategorySummary {
	if global == nil || totalErrors <= 0 {
		return nil
	}
	counters := sortedErrorCounters(global.ErrorCategories)
	summaries := make([]errorCategorySummary, 0, len(counters))
	for _, counter := range counters {
		summaries = append(summaries, errorCategorySummary{
			Category:        counter.Category,
			Count:           counter.Count,
			PercentOfErrors: safeDiv(float64(counter.Count)*100, float64(totalErrors)),
			FirstSecond:     counter.FirstSecond,
			LastSecond:      counter.LastSecond,
			Sample:          counter.Sample,
		})
	}
	return summaries
}

func buildTopErrorMessageSummaries(tracker *errorMessageTracker, totalErrors int64) []errorMessageSummary {
	if tracker == nil || totalErrors <= 0 {
		return nil
	}
	counters := sortedErrorCounters(tracker.entries)
	summaries := make([]errorMessageSummary, 0, len(counters))
	for _, counter := range counters {
		summaries = append(summaries, errorMessageSummary{
			Category:        counter.Category,
			Message:         counter.Message,
			Count:           counter.Count,
			PercentOfErrors: safeDiv(float64(counter.Count)*100, float64(totalErrors)),
			FirstSecond:     counter.FirstSecond,
			LastSecond:      counter.LastSecond,
			Sample:          counter.Sample,
		})
	}
	return summaries
}

func errorSecondRange(categories []errorCategorySummary) (*int, *int) {
	if len(categories) == 0 {
		return nil, nil
	}
	first := categories[0].FirstSecond
	last := categories[0].LastSecond
	for _, category := range categories[1:] {
		if category.FirstSecond < first {
			first = category.FirstSecond
		}
		if category.LastSecond > last {
			last = category.LastSecond
		}
	}
	return &first, &last
}

func untrackedErrorMessages(tracker *errorMessageTracker) int64 {
	if tracker == nil {
		return 0
	}
	return tracker.untrackedSamples
}

func buildClientSaturationSummary(cfg config, service latencySummary, startLag latencySummary, wallSeconds float64) clientSaturationSummary {
	drainSeconds := math.Max(0, wallSeconds-cfg.Duration.Seconds())
	return clientSaturationSummary{
		WorkerStarved:                    isWorkerStarved(startLag, drainSeconds),
		CompletedWithinScheduledDuration: drainSeconds <= cfg.Duration.Seconds()*0.01,
		DrainSeconds:                     drainSeconds,
		RecommendedWorkersForServiceP95:  recommendedWorkers(cfg.Rate, service.P95Ms, cfg.WorkerHeadroom),
		RecommendedWorkersForServiceP99:  recommendedWorkers(cfg.Rate, service.P99Ms, cfg.WorkerHeadroom),
		RecommendedWorkersForServiceP999: recommendedWorkers(cfg.Rate, service.P999Ms, cfg.WorkerHeadroom),
	}
}

// buildRunnerRuntimeSummary snapshots Go runtime and benchmark queue counters
// after the run has drained.
func buildRunnerRuntimeSummary(stats *runStats) runnerRuntimeSummary {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	scheduler := stats.Scheduler
	return runnerRuntimeSummary{
		HeapAllocBytes:            mem.HeapAlloc,
		HeapSysBytes:              mem.HeapSys,
		SysBytes:                  mem.Sys,
		NumGC:                     mem.NumGC,
		Goroutines:                runtime.NumGoroutine(),
		RequestQueueCapacity:      stats.RequestQueueCapacity,
		RequestQueuePeakDepth:     scheduler.requestQueuePeakDepth.Load(),
		ResultQueueCapacity:       stats.ResultQueueCapacity,
		ResultQueuePeakDepth:      scheduler.resultQueuePeakDepth.Load(),
		SchedulerMaxLagMs:         nsToMs(float64(scheduler.schedulerMaxLagMicros.Load()) * float64(time.Microsecond)),
		SchedulerBlockedCount:     scheduler.schedulerBlockedCount.Load(),
		SchedulerBlockedSeconds:   float64(scheduler.schedulerBlockedNanos.Load()) / float64(time.Second),
		ResultQueueBlockedCount:   scheduler.resultQueueBlockedCount.Load(),
		ResultQueueBlockedSeconds: float64(scheduler.resultQueueBlockedNanos.Load()) / float64(time.Second),
		MetricsFlushes:            scheduler.metricsFlushes.Load(),
	}
}

func buildWarnings(cfg config, corrected latencySummary, service latencySummary, startLag latencySummary, saturation clientSaturationSummary, runtimeSummary runnerRuntimeSummary) []string {
	warnings := []string{}
	if saturation.WorkerStarved {
		warnings = append(warnings, fmt.Sprintf(
			"client worker starvation detected: start-lag p95 %.3fms and drain %.3fs; increase --workers or --connections",
			startLag.P95Ms,
			saturation.DrainSeconds,
		))
	}
	if cfg.Workers < saturation.RecommendedWorkersForServiceP99 && saturation.WorkerStarved {
		warnings = append(warnings, fmt.Sprintf(
			"configured workers=%d is below the observed service-p99 recommendation=%d",
			cfg.Workers,
			saturation.RecommendedWorkersForServiceP99,
		))
	}
	if corrected.P99Ms > service.P99Ms*2 && startLag.P95Ms > 10 {
		warnings = append(warnings, "corrected latency is much higher than service latency; the run includes client-side queueing/backlog")
	}
	if runtimeSummary.SchedulerBlockedCount > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"scheduler experienced bounded-queue backpressure %.3fs across %d sends; corrected latency preserves the original schedule and includes this lag",
			runtimeSummary.SchedulerBlockedSeconds,
			runtimeSummary.SchedulerBlockedCount,
		))
	}
	if runtimeSummary.SchedulerMaxLagMs > 10 {
		warnings = append(warnings, fmt.Sprintf(
			"scheduler max dispatch lag was %.3fms; the offered schedule was preserved and corrected latency includes this lag",
			runtimeSummary.SchedulerMaxLagMs,
		))
	}
	if runtimeSummary.ResultQueueBlockedCount > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"result collector queue blocked %.3fs across %d sends; runner-side collection may be saturated",
			runtimeSummary.ResultQueueBlockedSeconds,
			runtimeSummary.ResultQueueBlockedCount,
		))
	}
	if runtimeSummary.RequestQueueCapacity > max(cfg.Workers*32, cfg.Rate) {
		warnings = append(warnings, fmt.Sprintf(
			"large request queue capacity=%d can consume unnecessary client memory; default workers*2 is recommended for steady-state benchmark runs",
			runtimeSummary.RequestQueueCapacity,
		))
	}
	return warnings
}

func isWorkerStarved(startLag latencySummary, drainSeconds float64) bool {
	return startLag.P95Ms > 25 || drainSeconds > 0.5
}

func recommendedWorkers(rate int, latencyMs float64, headroom float64) int {
	if rate <= 0 || latencyMs <= 0 {
		return 1
	}
	return max(1, int(math.Ceil(float64(rate)*latencyMs/1000.0*headroom)))
}

// requestQueueCapacity keeps the scheduled operation backlog bounded by
// default. Large explicit values remain possible for experiments but are
// visible in summary guardrails.
func requestQueueCapacity(cfg config) int {
	if cfg.QueueCap > 0 {
		return max(1, cfg.QueueCap)
	}
	return max(1, cfg.Workers*2)
}

// resultQueueCapacity bounds completed operation buffering between workers and
// the collector.
func resultQueueCapacity(cfg config) int {
	return max(1, cfg.Workers*2)
}

// atomicMaxInt64 records a lock-free maximum from concurrent hot paths.
func atomicMaxInt64(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current {
			return
		}
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

// recordQueueBlock records meaningful blocking time while ignoring scheduler
// noise below 100 microseconds.
func recordQueueBlock(count *atomic.Int64, totalNanos *atomic.Int64, elapsed time.Duration) {
	if elapsed <= 100*time.Microsecond {
		return
	}
	count.Add(1)
	totalNanos.Add(elapsed.Nanoseconds())
}

func writeJSON(path string, value any) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// summarizeHistogram converts a histogram snapshot into the percentile fields
// used by metrics.csv and summary.json.
func summarizeHistogram(histogram *hdrhistogram.Histogram) latencySummary {
	if histogram == nil || histogram.TotalCount() == 0 {
		return latencySummary{}
	}
	return latencySummary{
		AvgMs:  microsToMs(histogram.Mean()),
		MinMs:  microsToMs(float64(histogram.Min())),
		P50Ms:  microsToMs(float64(histogram.ValueAtPercentile(50))),
		P75Ms:  microsToMs(float64(histogram.ValueAtPercentile(75))),
		P90Ms:  microsToMs(float64(histogram.ValueAtPercentile(90))),
		P95Ms:  microsToMs(float64(histogram.ValueAtPercentile(95))),
		P99Ms:  microsToMs(float64(histogram.ValueAtPercentile(99))),
		P999Ms: microsToMs(float64(histogram.ValueAtPercentile(99.9))),
		MaxMs:  microsToMs(float64(histogram.Max())),
	}
}

// summarizeLatency is retained for unit tests that describe latency samples in
// nanoseconds; production code records directly into histograms.
func summarizeLatency(values []int64) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	histogram := newLatencyHistogram(globalLatencySigfigs)
	for _, value := range values {
		recordDuration(histogram, time.Duration(max64(value, 0)))
	}
	return summarizeHistogram(histogram)
}

func microsToMs(micros float64) float64 {
	return micros / 1000.0
}

func nsToMs(ns float64) float64 {
	return ns / float64(time.Millisecond)
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func maxSecond(buckets map[int]*bucket) int {
	maxSec := -1
	for sec := range buckets {
		if sec > maxSec {
			maxSec = sec
		}
	}
	return maxSec
}

func sortedSeconds(buckets map[int]*bucket) []int {
	seconds := make([]int, 0, len(buckets))
	for sec := range buckets {
		seconds = append(seconds, sec)
	}
	sort.Ints(seconds)
	return seconds
}

func sortedErrorCounters(counters map[string]*errorCounter) []*errorCounter {
	out := make([]*errorCounter, 0, len(counters))
	for _, counter := range counters {
		if counter != nil && counter.Count > 0 {
			out = append(out, counter)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].Category != out[j].Category {
			return out[i].Category < out[j].Category
		}
		return out[i].Message < out[j].Message
	})
	return out
}

// operationBuilder creates operation keys and values in worker goroutines, so
// queued schedule slots do not retain per-operation strings.
type operationBuilder struct {
	cfg      config
	valuePad string
}

func newOperationBuilder(cfg config) operationBuilder {
	return operationBuilder{
		cfg:      cfg,
		valuePad: strings.Repeat("x", max(cfg.ValueBytes, 0)),
	}
}

func (b operationBuilder) key(idx int) string {
	return keyFor(b.cfg, idx)
}

func (b operationBuilder) value(seq int64) string {
	if b.cfg.ValueBytes == 0 {
		return ""
	}
	prefix := fmt.Sprintf("v%020d:", seq)
	if len(prefix) >= b.cfg.ValueBytes {
		return prefix[:b.cfg.ValueBytes]
	}
	return prefix + b.valuePad[:b.cfg.ValueBytes-len(prefix)]
}

func keyFor(cfg config, idx int) string {
	return fmt.Sprintf("%s%010d", cfg.KeyPrefix, idx)
}

func valueFor(cfg config, seq int64) string {
	return newOperationBuilder(cfg).value(seq)
}

type etcdClient struct {
	client            *clientv3.Client
	serializableReads bool
}

func (c *etcdClient) Get(ctx context.Context, key string) error {
	options := []clientv3.OpOption{}
	if c.serializableReads {
		options = append(options, clientv3.WithSerializable())
	}
	_, err := c.client.Get(ctx, key, options...)
	return err
}

func (c *etcdClient) Put(ctx context.Context, key string, value string) error {
	_, err := c.client.Put(ctx, key, value)
	return err
}

func (c *etcdClient) Close() error {
	return c.client.Close()
}

type respClient struct {
	endpoint string
	timeout  time.Duration
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
}

func newRESPClient(endpoint string, timeout time.Duration) *respClient {
	return &respClient{endpoint: endpoint, timeout: timeout}
}

func (c *respClient) Get(ctx context.Context, key string) error {
	_, err := c.do(ctx, "GET", key)
	return err
}

func (c *respClient) Put(ctx context.Context, key string, value string) error {
	response, err := c.do(ctx, "SET", key, value)
	if err != nil {
		return err
	}
	if response != "OK" {
		return fmt.Errorf("unexpected SET response: %q", response)
	}
	return nil
}

func (c *respClient) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.reader = nil
	c.writer = nil
	return err
}

func (c *respClient) do(ctx context.Context, args ...string) (string, error) {
	if err := c.ensureConnected(ctx); err != nil {
		return "", err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
	}
	if err := writeRESPCommand(c.writer, args); err != nil {
		_ = c.Close()
		return "", err
	}
	if err := c.writer.Flush(); err != nil {
		_ = c.Close()
		return "", err
	}
	response, err := readRESPResponse(c.reader)
	if err != nil {
		_ = c.Close()
		return "", err
	}
	return response, nil
}

func (c *respClient) ensureConnected(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	dialer := net.Dialer{Timeout: c.timeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.endpoint)
	if err != nil {
		return err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	return nil
}

func writeRESPCommand(writer *bufio.Writer, args []string) error {
	if _, err := fmt.Fprintf(writer, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

func readRESPResponse(reader *bufio.Reader) (string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	switch prefix {
	case '+':
		return line, nil
	case '-':
		return "", errors.New(line)
	case ':':
		return line, nil
	case '$':
		length, err := strconv.Atoi(line)
		if err != nil {
			return "", fmt.Errorf("invalid bulk string length %q: %w", line, err)
		}
		if length == -1 {
			return "", nil
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return "", err
		}
		return string(buf[:length]), nil
	default:
		return "", fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func safeDiv(numerator, denominator float64) float64 {
	if denominator <= 0 {
		return 0
	}
	return numerator / denominator
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
