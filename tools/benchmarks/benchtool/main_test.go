package main

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSummarizeLatencyPercentiles(t *testing.T) {
	values := []int64{
		int64(1 * time.Millisecond),
		int64(2 * time.Millisecond),
		int64(3 * time.Millisecond),
		int64(4 * time.Millisecond),
	}
	stats := summarizeLatency(values)

	if !floatClose(stats.AvgMs, 2.5, 0.01) {
		t.Fatalf("avg = %v, want 2.5", stats.AvgMs)
	}
	if !floatClose(stats.MinMs, 1, 0.01) {
		t.Fatalf("min = %v, want 1", stats.MinMs)
	}
	if !floatClose(stats.P50Ms, 2, 0.01) {
		t.Fatalf("p50 = %v, want 2", stats.P50Ms)
	}
	if !floatClose(stats.P75Ms, 3, 0.01) {
		t.Fatalf("p75 = %v, want 3", stats.P75Ms)
	}
	if !floatClose(stats.P99Ms, 4, 0.01) {
		t.Fatalf("p99 = %v, want 4", stats.P99Ms)
	}
	if !floatClose(stats.MaxMs, 4, 0.01) {
		t.Fatalf("max = %v, want 4", stats.MaxMs)
	}
}

func floatClose(got float64, want float64, tolerance float64) bool {
	return math.Abs(got-want) <= tolerance
}

func TestParseWorkersRequiresExplicitPositiveInteger(t *testing.T) {
	workers, err := parseRequiredPositiveInt("--workers", "128")
	if err != nil {
		t.Fatalf("parse workers: %v", err)
	}
	if workers != 128 {
		t.Fatalf("workers = %d, want 128", workers)
	}
}

func TestParseWorkersRejectsAuto(t *testing.T) {
	if _, err := parseRequiredPositiveInt("--workers", "auto"); err == nil {
		t.Fatalf("parse auto workers succeeded, want error")
	}
}

func TestResolveConnectionsDefaultsToWorkers(t *testing.T) {
	connections, err := resolveConnections("", 64)
	if err != nil {
		t.Fatalf("resolve default connections: %v", err)
	}
	if connections != 64 {
		t.Fatalf("connections = %d, want 64", connections)
	}
}

func TestResolveConnectionsManual(t *testing.T) {
	connections, err := resolveConnections("16", 64)
	if err != nil {
		t.Fatalf("resolve manual connections: %v", err)
	}
	if connections != 16 {
		t.Fatalf("connections = %d, want 16", connections)
	}
}

func TestRequestQueueCapacityDefaultsToBoundedWorkerMultiple(t *testing.T) {
	cfg := config{Workers: 64}
	if got := requestQueueCapacity(cfg); got != 128 {
		t.Fatalf("request queue cap = %d, want 128", got)
	}
}

func TestRequestQueueCapacityHonorsExplicitValue(t *testing.T) {
	cfg := config{Workers: 64, QueueCap: 17}
	if got := requestQueueCapacity(cfg); got != 17 {
		t.Fatalf("request queue cap = %d, want 17", got)
	}
}

func TestBuildWarningsDetectsWorkerStarvation(t *testing.T) {
	cfg := config{
		Rate:           1000,
		Workers:        64,
		Connections:    64,
		Duration:       15 * time.Second,
		WorkerHeadroom: 1.0,
	}
	service := latencySummary{P95Ms: 100, P99Ms: 200, P999Ms: 300}
	startLag := latencySummary{P95Ms: 250}
	saturation := buildClientSaturationSummary(cfg, service, startLag, 20)
	warnings := buildWarnings(cfg, latencySummary{P99Ms: 1000}, service, startLag, saturation, runnerRuntimeSummary{})
	if !saturation.WorkerStarved {
		t.Fatalf("WorkerStarved = false, want true")
	}
	if len(warnings) == 0 {
		t.Fatalf("warnings empty, want worker starvation warning")
	}
}

func TestValueForHonorsConfiguredSize(t *testing.T) {
	cfg := config{ValueBytes: 32}
	value := valueFor(cfg, 7)
	if len(value) != 32 {
		t.Fatalf("value len = %d, want 32", len(value))
	}

	cfg.ValueBytes = 4
	value = valueFor(cfg, 7)
	if len(value) != 4 {
		t.Fatalf("truncated value len = %d, want 4", len(value))
	}
}

func TestNormalizeEtcdEndpointsAddsScheme(t *testing.T) {
	got := normalizeEtcdEndpoints([]string{"etcd1:2379", "http://etcd2:2379"})
	want := []string{"http://etcd1:2379", "http://etcd2:2379"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("endpoint %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPreloadWorkerCountDefaultsConservatively(t *testing.T) {
	cfg := config{Workers: 64}
	if got := preloadWorkerCount(cfg); got != 16 {
		t.Fatalf("preload workers = %d, want 16", got)
	}

	cfg.PreloadWorkers = 7
	if got := preloadWorkerCount(cfg); got != 7 {
		t.Fatalf("explicit preload workers = %d, want 7", got)
	}
}

func TestParseRunArgsAcceptsPreloadControls(t *testing.T) {
	cfg, err := parseRunArgs([]string{
		"--target", "holostore",
		"--endpoints", "holostore1:6379",
		"--workers", "16",
		"--preload-workers", "9",
		"--preload-timeout", "45s",
		"--preload-retries", "5",
	})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.PreloadWorkers != 9 {
		t.Fatalf("preload workers = %d, want 9", cfg.PreloadWorkers)
	}
	if cfg.PreloadTimeout != 45*time.Second {
		t.Fatalf("preload timeout = %s, want 45s", cfg.PreloadTimeout)
	}
	if cfg.PreloadRetries != 5 {
		t.Fatalf("preload retries = %d, want 5", cfg.PreloadRetries)
	}
}

func TestParseRunArgsAcceptsConnectionControls(t *testing.T) {
	cfg, err := parseRunArgs([]string{
		"--target", "etcd",
		"--endpoints", "etcd1:2379",
		"--workers", "64",
		"--connections", "8",
		"--worker-headroom", "1.5",
	})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.Workers != 64 {
		t.Fatalf("workers = %d, want 64", cfg.Workers)
	}
	if cfg.Connections != 8 {
		t.Fatalf("connections = %d, want 8", cfg.Connections)
	}
	if cfg.WorkerHeadroom != 1.5 {
		t.Fatalf("worker headroom = %v, want 1.5", cfg.WorkerHeadroom)
	}
}

func TestParseRunArgsRejectsMissingWorkers(t *testing.T) {
	if _, err := parseRunArgs([]string{"--target", "holostore", "--endpoints", "holostore1:6379"}); err == nil {
		t.Fatalf("parse args without workers succeeded, want error")
	}
}

func TestRunLoadUsesConnectionPoolSize(t *testing.T) {
	created := 0
	cfg := config{
		Target:         targetHoloStore,
		Scenario:       "pool-test",
		Rate:           10,
		Duration:       100 * time.Millisecond,
		DurationRaw:    "100ms",
		Workers:        5,
		Connections:    2,
		WorkerHeadroom: 1.0,
		WritePct:       100,
		Contention:     "uniform",
		Keys:           1,
		ValueBytes:     8,
		Timeout:        time.Second,
		TimeoutRaw:     "1s",
		Progress:       false,
	}
	outDir := t.TempDir()
	metricsPath := outDir + "/metrics.csv"
	errorsPath := outDir + "/errors.csv"
	stats, err := runLoad(cfg, func(int) (kvClient, error) {
		created++
		return &noopClient{}, nil
	}, metricsPath, errorsPath)
	if err != nil {
		t.Fatalf("run load: %v", err)
	}
	if created != 2 {
		t.Fatalf("created clients = %d, want 2", created)
	}
	if stats.RequestQueueCapacity != 10 {
		t.Fatalf("request queue cap = %d, want 10", stats.RequestQueueCapacity)
	}
	metrics, err := os.ReadFile(metricsPath)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	if !strings.Contains(string(metrics), "target,scenario,second") {
		t.Fatalf("metrics header missing: %s", metrics)
	}
	errorsCSV, err := os.ReadFile(errorsPath)
	if err != nil {
		t.Fatalf("read errors csv: %v", err)
	}
	if !strings.Contains(string(errorsCSV), "target,scenario,second,category,count,sample") {
		t.Fatalf("errors header missing: %s", errorsCSV)
	}
	if stats.Global.Completed == 0 {
		t.Fatalf("global completed = 0, want completed operations")
	}
}

func TestClassifyErrorCategories(t *testing.T) {
	cases := map[string]string{
		"ERR proposal timed out":                                       "resp_error",
		"unexpected SET response: \"QUEUED\"":                          "unexpected_response",
		"acquire connection: context deadline exceeded":                "pool_timeout",
		"unsupported RESP prefix '?'":                                  "protocol_error",
		"rpc error: code = ResourceExhausted desc = too many writes":   "etcd_resource_exhausted",
		"rpc error: code = Unavailable desc = transport is closing":    "etcd_unavailable",
		"read tcp 127.0.0.1:1->127.0.0.1:2: i/o timeout":               "timeout",
		"write tcp 127.0.0.1:1->127.0.0.1:2: connection reset by peer": "connection_error",
		"mystery failure": "other",
	}
	for message, want := range cases {
		if got := classifyError(message); got != want {
			t.Fatalf("classifyError(%q) = %q, want %q", message, got, want)
		}
	}
}

func TestErrorAggregationSummarizesCategoriesAndMessages(t *testing.T) {
	cfg := config{
		Target:         targetHoloStore,
		Scenario:       "error-test",
		Rate:           1000,
		Duration:       time.Second,
		DurationRaw:    "1s",
		Workers:        1,
		Connections:    1,
		WorkerHeadroom: 1,
		WritePct:       100,
		Contention:     "uniform",
		Keys:           1,
		Timeout:        time.Second,
		TimeoutRaw:     "1s",
	}
	global := newBucket(globalLatencySigfigs)
	secondBucket := newBucket(bucketLatencySigfigs)
	tracker := newErrorMessageTracker()
	res := result{
		Op:             opWrite,
		Latency:        3 * time.Millisecond,
		ServiceLatency: 2 * time.Millisecond,
		StartLag:       time.Millisecond,
		Err:            "ERR proposal timed out",
	}
	category := classifyError(res.Err)
	recordResult(global, res, 7, category)
	recordResult(secondBucket, res, 7, category)
	tracker.Record(category, res.Err, 7)

	stats := &runStats{
		Buckets:              map[int]*bucket{7: secondBucket},
		Global:               global,
		ErrorMessages:        tracker,
		ScheduledStart:       time.Now(),
		CompletedAt:          time.Now().Add(time.Second),
		TotalRequests:        1,
		RequestQueueCapacity: 1,
		ResultQueueCapacity:  1,
		Scheduler:            &schedulerStats{},
	}
	summary := buildSummary(cfg, stats, "/tmp/metrics.csv", "/tmp/errors.csv", "/tmp/config.json")
	if len(summary.ErrorCategories) != 1 {
		t.Fatalf("error categories len = %d, want 1", len(summary.ErrorCategories))
	}
	if summary.ErrorCategories[0].Category != "resp_error" || summary.ErrorCategories[0].Count != 1 {
		t.Fatalf("unexpected error category: %+v", summary.ErrorCategories[0])
	}
	if summary.FirstErrorSecond == nil || *summary.FirstErrorSecond != 7 {
		t.Fatalf("first error second = %v, want 7", summary.FirstErrorSecond)
	}
	if len(summary.TopErrorMessages) != 1 {
		t.Fatalf("top error messages len = %d, want 1", len(summary.TopErrorMessages))
	}
	if summary.TopErrorMessages[0].Sample != "ERR proposal timed out" {
		t.Fatalf("top error sample = %q", summary.TopErrorMessages[0].Sample)
	}
}

func TestWriteErrorsCSVIncludesPerSecondCategories(t *testing.T) {
	cfg := config{Target: targetHoloStore, Scenario: "error-csv"}
	resultBucket := newBucket(bucketLatencySigfigs)
	res := result{
		Op:             opWrite,
		Latency:        time.Millisecond,
		ServiceLatency: time.Millisecond,
		Err:            "read tcp 127.0.0.1:1->127.0.0.1:2: i/o timeout",
	}
	recordResult(resultBucket, res, 3, classifyError(res.Err))

	path := t.TempDir() + "/errors.csv"
	if err := writeErrorsCSVAtomic(path, cfg, map[int]*bucket{3: resultBucket}); err != nil {
		t.Fatalf("write errors csv: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read errors csv: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "holostore,error-csv,3,timeout,1,") {
		t.Fatalf("errors csv row missing: %s", text)
	}
}

func TestPreloadPutWithRetriesRetriesTransientFailures(t *testing.T) {
	client := &flakyClient{failuresRemaining: 2}
	cfg := config{
		KeyPrefix:      "k_",
		ValueBytes:     8,
		PreloadTimeout: time.Second,
		PreloadRetries: 3,
	}
	attempts, err := preloadPutWithRetries(cfg, client, 1)
	if err != nil {
		t.Fatalf("preload put: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if client.puts != 3 {
		t.Fatalf("puts = %d, want 3", client.puts)
	}
}

type noopClient struct{}

func (c *noopClient) Get(context.Context, string) error {
	return nil
}

func (c *noopClient) Put(context.Context, string, string) error {
	return nil
}

func (c *noopClient) Close() error {
	return nil
}

type flakyClient struct {
	failuresRemaining int
	puts              int
}

func (c *flakyClient) Get(context.Context, string) error {
	return nil
}

func (c *flakyClient) Put(context.Context, string, string) error {
	c.puts++
	if c.failuresRemaining > 0 {
		c.failuresRemaining--
		return errors.New("transient")
	}
	return nil
}

func (c *flakyClient) Close() error {
	return nil
}

func BenchmarkRecordResultHistogram(b *testing.B) {
	bucket := newBucket(bucketLatencySigfigs)
	res := result{
		Op:             opWrite,
		Latency:        3 * time.Millisecond,
		ServiceLatency: 2 * time.Millisecond,
		StartLag:       time.Millisecond,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		recordResult(bucket, res, 0, "")
	}
}

func BenchmarkRecordResultErrorCategorized(b *testing.B) {
	bucket := newBucket(bucketLatencySigfigs)
	res := result{
		Op:             opWrite,
		Latency:        3 * time.Millisecond,
		ServiceLatency: 2 * time.Millisecond,
		StartLag:       time.Millisecond,
		Err:            "ERR proposal timed out",
	}
	category := classifyError(res.Err)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		recordResult(bucket, res, 0, category)
	}
}
