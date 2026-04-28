package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

// History is the JSON payload written by holo-workload.
type History struct {
	Meta Meta       `json:"meta"`
	Ops  []OpRecord `json:"ops"`
}

// Meta captures reproducibility inputs for one workload run.
type Meta struct {
	Nodes              []string `json:"nodes"`
	ReadNodes          []string `json:"read_nodes"`
	WriteNodes         []string `json:"write_nodes"`
	Clients            int      `json:"clients"`
	Keys               int      `json:"keys"`
	KeyPrefix          string   `json:"key_prefix"`
	ValuePrefix        string   `json:"value_prefix"`
	SetPct             int      `json:"set_pct"`
	DurationMs         int64    `json:"duration_ms"`
	Seed               uint64   `json:"seed"`
	FaultDisconnectPct int      `json:"fault_disconnect_pct"`
	ChecksumValues     bool     `json:"checksum_values"`
}

// OpRecord describes one client-visible GET or SET.
type OpRecord struct {
	Client   int      `json:"client"`
	Node     string   `json:"node"`
	Op       string   `json:"op"`
	Key      string   `json:"key"`
	Value    *string  `json:"value"`
	CallUs   int64    `json:"call_us"`
	ReturnUs int64    `json:"return_us"`
	Result   OpResult `json:"result"`
}

// OpResult records a structured outcome that can be checked later.
type OpResult struct {
	Type  string  `json:"type"`
	Value *string `json:"value"`
	Error *string `json:"error"`
}

// CheckSummary is a machine-readable result for Antithesis drivers.
type CheckSummary struct {
	Ok                      bool    `json:"ok"`
	KeysChecked             int     `json:"keys_checked"`
	OpsChecked              int     `json:"ops_checked"`
	ErrorsIgnored           int     `json:"errors_ignored"`
	OutOfThinAir            bool    `json:"out_of_thin_air"`
	FailureVisualization    *string `json:"failure_visualization"`
	ChecksumValuesValidated bool    `json:"checksum_values_validated"`
}

type regInput struct {
	Kind  string
	Value string
}

type regOutput struct {
	Kind  string
	Value string
}

type regState struct {
	Has   bool
	Value string
}

// ChecksummedValue is the schema emitted by holo-workload --checksum-values.
type ChecksummedValue struct {
	Scenario string
	Key      string
	Client   string
	Seq      string
	Checksum string
}

func main() {
	var historyPath string
	var jsonSummaryPath string
	var allowErrors bool
	flag.StringVar(&historyPath, "history", defaultHistoryPath(), "path to history JSON")
	flag.StringVar(&jsonSummaryPath, "json-summary", "", "optional output path for checker summary JSON")
	flag.BoolVar(&allowErrors, "allow-errors", false, "ignore errored operations (still checks successful ops)")
	flag.Parse()

	summary := &CheckSummary{}
	exitCode := run(historyPath, allowErrors, summary)
	if jsonSummaryPath != "" {
		if err := writeSummary(jsonSummaryPath, summary); err != nil {
			fmt.Fprintf(os.Stderr, "error: write json summary: %v\n", err)
			if exitCode == 0 {
				exitCode = 2
			}
		}
	}
	os.Exit(exitCode)
}

// run performs the actual validation and returns a shell exit code.
func run(historyPath string, allowErrors bool, summary *CheckSummary) int {
	history, err := readHistory(historyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 2
	}

	for _, op := range history.Ops {
		if op.Result.Type == "err" {
			if allowErrors {
				summary.ErrorsIgnored++
				continue
			}
			fmt.Fprintf(os.Stderr, "error: history contains errors (use --allow-errors to ignore)\n")
			fmt.Fprintf(os.Stderr, "example: client=%d node=%s op=%s key=%s err=%s\n",
				op.Client, op.Node, op.Op, op.Key, deref(op.Result.Error))
			return 1
		}
	}

	attempted := map[string]map[string]bool{}
	checksumValidated := false
	for _, op := range history.Ops {
		if op.Op != "set" || op.Value == nil {
			continue
		}
		if looksLikeChecksummedValue(*op.Value) {
			checksumValidated = true
			if err := validateChecksummedValue(*op.Value, op.Key); err != nil {
				fmt.Fprintf(os.Stderr, "FAIL: invalid SET checksum payload for key=%s: %v\n", op.Key, err)
				return 1
			}
		}

		m, ok := attempted[op.Key]
		if !ok {
			m = map[string]bool{}
			attempted[op.Key] = m
		}
		m[*op.Value] = true
	}

	for _, op := range history.Ops {
		if op.Op != "get" || op.Result.Type != "value" || op.Result.Value == nil {
			continue
		}
		if looksLikeChecksummedValue(*op.Result.Value) {
			checksumValidated = true
			if err := validateChecksummedValue(*op.Result.Value, op.Key); err != nil {
				fmt.Fprintf(os.Stderr, "FAIL: invalid GET checksum payload for key=%s: %v\n", op.Key, err)
				return 1
			}
		}

		m := attempted[op.Key]
		if m == nil || !m[*op.Result.Value] {
			summary.OutOfThinAir = true
			fmt.Fprintf(os.Stderr, "FAIL: GET returned value never written: key=%s value=%s\n", op.Key, *op.Result.Value)
			return 1
		}
	}

	opsByKey := map[string][]porcupine.Operation{}
	for _, op := range history.Ops {
		porcOp, ok := toPorcupineOperation(op)
		if !ok {
			continue
		}
		opsByKey[op.Key] = append(opsByKey[op.Key], porcOp)
	}

	keys := make([]string, 0, len(opsByKey))
	for k := range opsByKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	model := registerModel()
	summary.KeysChecked = len(keys)
	summary.OpsChecked = countOps(opsByKey)
	summary.ChecksumValuesValidated = checksumValidated

	for _, key := range keys {
		ops := opsByKey[key]
		if len(ops) == 0 {
			continue
		}
		res, info := porcupine.CheckOperationsVerbose(model, ops, 0)
		if res != porcupine.Ok {
			outDir := filepath.Dir(historyPath)
			if outDir == "" || outDir == "." {
				outDir = "."
			}
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				fmt.Fprintf(os.Stderr, "error: mkdir %s: %v\n", outDir, err)
				return 2
			}
			out := filepath.Join(outDir, fmt.Sprintf("failure-%s.html", key))
			_ = porcupine.VisualizePath(model, info, out)
			summary.FailureVisualization = &out
			fmt.Fprintf(os.Stderr, "FAIL: key=%s is not linearizable (result=%s)\n", key, res)
			fmt.Fprintf(os.Stderr, "wrote visualization: %s\n", out)
			return 1
		}
	}

	summary.Ok = true
	fmt.Printf("OK: linearizable for %d keys (%d ops)\n", len(keys), summary.OpsChecked)
	return 0
}

func countOps(m map[string][]porcupine.Operation) int {
	total := 0
	for _, v := range m {
		total += len(v)
	}
	return total
}

func toPorcupineOperation(op OpRecord) (porcupine.Operation, bool) {
	switch op.Op {
	case "set":
		if op.Result.Type != "ok" || op.Value == nil {
			return porcupine.Operation{}, false
		}
		return porcupine.Operation{
			ClientId: op.Client,
			Input:    regInput{Kind: "set", Value: *op.Value},
			Output:   regOutput{Kind: "ok"},
			Call:     op.CallUs,
			Return:   op.ReturnUs,
		}, true
	case "get":
		switch op.Result.Type {
		case "nil":
			return porcupine.Operation{
				ClientId: op.Client,
				Input:    regInput{Kind: "get"},
				Output:   regOutput{Kind: "nil"},
				Call:     op.CallUs,
				Return:   op.ReturnUs,
			}, true
		case "value":
			if op.Result.Value == nil {
				return porcupine.Operation{}, false
			}
			return porcupine.Operation{
				ClientId: op.Client,
				Input:    regInput{Kind: "get"},
				Output:   regOutput{Kind: "value", Value: *op.Result.Value},
				Call:     op.CallUs,
				Return:   op.ReturnUs,
			}, true
		default:
			return porcupine.Operation{}, false
		}
	default:
		return porcupine.Operation{}, false
	}
}

func registerModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return regState{Has: false}
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			s := state.(regState)
			in := input.(regInput)
			out := output.(regOutput)

			switch in.Kind {
			case "set":
				if out.Kind != "ok" {
					return false, state
				}
				return true, regState{Has: true, Value: in.Value}
			case "get":
				if !s.Has {
					return out.Kind == "nil", state
				}
				return out.Kind == "value" && out.Value == s.Value, state
			default:
				return false, state
			}
		},
		Equal: func(a interface{}, b interface{}) bool {
			aa := a.(regState)
			bb := b.(regState)
			return aa.Has == bb.Has && aa.Value == bb.Value
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			in := input.(regInput)
			out := output.(regOutput)
			if in.Kind == "set" {
				return fmt.Sprintf("SET(%s)->%s", in.Value, out.Kind)
			}
			if out.Kind == "value" {
				return fmt.Sprintf("GET()->%s", out.Value)
			}
			return fmt.Sprintf("GET()->%s", out.Kind)
		},
	}
}

// parseChecksummedValue extracts the structured payload emitted by checksum mode.
func parseChecksummedValue(value string) (*ChecksummedValue, error) {
	parts := strings.Split(value, ";")
	if len(parts) != 5 {
		return nil, fmt.Errorf("expected 5 fields, got %d", len(parts))
	}

	fields := map[string]string{}
	for _, part := range parts {
		key, raw, ok := strings.Cut(part, "=")
		if !ok || key == "" || raw == "" {
			return nil, fmt.Errorf("invalid field %q", part)
		}
		fields[key] = raw
	}

	required := []string{"scenario", "key", "client", "seq", "checksum"}
	for _, key := range required {
		if fields[key] == "" {
			return nil, fmt.Errorf("missing %s", key)
		}
	}

	return &ChecksummedValue{
		Scenario: fields["scenario"],
		Key:      fields["key"],
		Client:   fields["client"],
		Seq:      fields["seq"],
		Checksum: fields["checksum"],
	}, nil
}

// validateChecksummedValue enforces checksum correctness and key binding.
func validateChecksummedValue(value string, expectedKey string) error {
	parsed, err := parseChecksummedValue(value)
	if err != nil {
		return err
	}
	if parsed.Key != expectedKey {
		return fmt.Errorf("embedded key %q does not match operation key %q", parsed.Key, expectedKey)
	}
	prefix := fmt.Sprintf(
		"scenario=%s;key=%s;client=%s;seq=%s",
		parsed.Scenario,
		parsed.Key,
		parsed.Client,
		parsed.Seq,
	)
	digest := sha256.Sum256([]byte(prefix))
	if hex.EncodeToString(digest[:]) != parsed.Checksum {
		return errors.New("checksum mismatch")
	}
	return nil
}

func looksLikeChecksummedValue(value string) bool {
	return strings.HasPrefix(value, "scenario=") || strings.Contains(value, ";checksum=")
}

func readHistory(path string) (*History, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var h History
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	// Ensure deterministic ordering in verbose traces.
	sort.Slice(h.Ops, func(i, j int) bool {
		if h.Ops[i].CallUs != h.Ops[j].CallUs {
			return h.Ops[i].CallUs < h.Ops[j].CallUs
		}
		if h.Ops[i].Client != h.Ops[j].Client {
			return h.Ops[i].Client < h.Ops[j].Client
		}
		return h.Ops[i].ReturnUs < h.Ops[j].ReturnUs
	})
	return &h, nil
}

func writeSummary(path string, summary *CheckSummary) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func defaultHistoryPath() string {
	if root := findRepoRoot(); root != "" {
		return filepath.Join(root, ".tmp", "porcupine", "history.json")
	}
	return filepath.Join(".tmp", "porcupine", "history.json")
}

func findRepoRoot() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
