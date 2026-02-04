package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/anishathalye/porcupine"
)

type History struct {
	Meta Meta       `json:"meta"`
	Ops  []OpRecord `json:"ops"`
}

type Meta struct {
	Nodes      []string `json:"nodes"`
	Clients    int      `json:"clients"`
	Keys       int      `json:"keys"`
	SetPct     int      `json:"set_pct"`
	DurationMs int64    `json:"duration_ms"`
	Seed       uint64   `json:"seed"`
}

type OpRecord struct {
	Client   int        `json:"client"`
	Node     string     `json:"node"`
	Op       string     `json:"op"`
	Key      string     `json:"key"`
	Value    *string    `json:"value"`
	CallUs   int64      `json:"call_us"`
	ReturnUs int64      `json:"return_us"`
	Result   OpResult   `json:"result"`
}

type OpResult struct {
	Type  string  `json:"type"`
	Value *string `json:"value"`
	Error *string `json:"error"`
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

func main() {
	var historyPath string
	var allowErrors bool
	flag.StringVar(&historyPath, "history", ".tmp/porcupine/history.json", "path to history JSON")
	flag.BoolVar(&allowErrors, "allow-errors", false, "ignore errored operations (still checks successful ops)")
	flag.Parse()

	history, err := readHistory(historyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	if !allowErrors {
		for _, op := range history.Ops {
			if op.Result.Type == "err" {
				fmt.Fprintf(os.Stderr, "error: history contains errors (use --allow-errors to ignore)\n")
				fmt.Fprintf(os.Stderr, "example: client=%d node=%s op=%s key=%s err=%s\n",
					op.Client, op.Node, op.Op, op.Key, deref(op.Result.Error))
				os.Exit(1)
			}
		}
	}

	// Out-of-thin-air sanity check: any observed GET value must have been attempted by some SET
	// (successful or not). This is weaker than linearizability but catches obvious corruption.
	attempted := map[string]map[string]bool{}
	for _, op := range history.Ops {
		if op.Op != "set" || op.Value == nil {
			continue
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
		m := attempted[op.Key]
		if m == nil || !m[*op.Result.Value] {
			fmt.Fprintf(os.Stderr, "FAIL: GET returned value never written: key=%s value=%s\n", op.Key, *op.Result.Value)
			os.Exit(1)
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
			_ = os.MkdirAll(outDir, 0o755)
			out := filepath.Join(outDir, fmt.Sprintf("failure-%s.html", key))
			_ = porcupine.VisualizePath(model, info, out)
			fmt.Fprintf(os.Stderr, "FAIL: key=%s is not linearizable (result=%s)\n", key, res)
			fmt.Fprintf(os.Stderr, "wrote visualization: %s\n", out)
			os.Exit(1)
		}
	}

	fmt.Printf("OK: linearizable for %d keys (%d ops)\n", len(keys), countOps(opsByKey))
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

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
