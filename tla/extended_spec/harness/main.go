package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	rt "runtime/trace"
	"strings"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/rafttest"
)

type scenarioResolution struct {
	Name string
	Path string
}

type runConfig struct {
	Scenario       scenarioResolution
	ScenarioSet    string
	TraceOut       string
	RuntimeTrace   string
	MetadataOut    string
	Verbose        bool
	PrintScenario  bool
	SkipValidation bool
}

func main() {
	cfg := parseFlags()

	if cfg.PrintScenario {
		if err := listScenarios(); err != nil {
			log.Fatalf("list scenarios: %v", err)
		}
		return
	}

	if cfg.ScenarioSet != "" {
		if err := runSet(cfg); err != nil {
			log.Fatalf("run set failed: %v", err)
		}
		return
	}

	if err := run(cfg); err != nil {
		log.Fatalf("specula harness failed: %v", err)
	}
}

func parseFlags() runConfig {
	var (
		scenarioFlag     = flag.String("scenario", "basic_election", "Scenario name or path to a datadriven script.")
		setFlag          = flag.String("set", "", "Run a set of scenarios (e.g., 'progress'). Overrides -scenario.")
		traceOutFlag     = flag.String("out", "", "Path to NDJSON state trace output (defaults to traces/<scenario>.ndjson).")
		runtimeTraceFlag = flag.String("runtime-trace", "", "Optional Go runtime trace output (.out).")
		metadataFlag     = flag.String("meta", "", "Optional metadata sidecar path (defaults to <out>.meta.json).")
		listFlag         = flag.Bool("list", false, "List built-in scenarios and exit.")
		verboseFlag      = flag.Bool("verbose", false, "Print scenario handler output while running.")
		skipValidateFlag = flag.Bool("skip-validate", false, "Skip comparing handler output against expected blocks.")
	)
	flag.Parse()

	var resolution scenarioResolution
	var err error
	if *setFlag == "" {
		resolution, err = resolveScenario(*scenarioFlag)
		if err != nil {
			log.Fatalf("resolve scenario: %v", err)
		}
	}

	traceOut := *traceOutFlag
	// traceOut default is handled per-scenario in runSet or run if mostly empty,
	// but here we keep it for the single scenario case.
	if traceOut == "" && resolution.Name != "" {
		traceOut = filepath.Join("traces", resolution.Name+".ndjson")
	}
	metaOut := *metadataFlag
	if metaOut == "" && traceOut != "" {
		metaOut = traceOut + ".meta.json"
	}

	return runConfig{
		Scenario:       resolution,
		ScenarioSet:    *setFlag,
		TraceOut:       traceOut,
		RuntimeTrace:   *runtimeTraceFlag,
		MetadataOut:    metaOut,
		Verbose:        *verboseFlag,
		PrintScenario:  *listFlag,
		SkipValidation: *skipValidateFlag,
	}
}

func runSet(baseCfg runConfig) error {
	var scenarios []string
	var exclude map[string]bool

	switch baseCfg.ScenarioSet {
	case "progress":
		// All scenarios except the 9 incompatible ones
		exclude = map[string]bool{
			"prevote":                                true,
			"prevote_checkquorum":                    true,
			"checkquorum":                            true,
			"forget_leader_prevote_checkquorum":      true,
			"slow_follower_after_compaction":         true,
			"snapshot_succeed_via_app_resp_behind":  true,
			"confchange_v2_add_double_auto":          true,
			"confchange_v2_add_double_implicit":      true,
			"confchange_v2_add_single_explicit":      true,
		}
	case "all":
		exclude = map[string]bool{}
	default:
		return fmt.Errorf("unknown set %q", baseCfg.ScenarioSet)
	}

	allScenarios := scenarioMap()
	for name := range allScenarios {
		if !exclude[name] {
			scenarios = append(scenarios, name)
		}
	}

	var failed []string
	for _, name := range scenarios {
		path := allScenarios[name]
		fmt.Printf("Running scenario: %s\n", name)
		
		// Derive config for this specific scenario
		cfg := baseCfg
		cfg.Scenario = scenarioResolution{Name: name, Path: path}
		// Reset outputs to defaults for this scenario
		cfg.TraceOut = filepath.Join("traces", name+".ndjson")
		cfg.MetadataOut = cfg.TraceOut + ".meta.json"
		
		if err := run(cfg); err != nil {
			fmt.Printf("FAIL: %s: %v\n", name, err)
			failed = append(failed, name)
		} else {
			fmt.Printf("PASS: %s\n", name)
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("scenarios failed: %v", failed)
	}
	return nil
}

func run(cfg runConfig) error {
	directives, err := loadDirectives(cfg.Scenario.Path)
	if err != nil {
		return fmt.Errorf("load scenario: %w", err)
	}
	if len(directives) == 0 {
		return fmt.Errorf("scenario %q (%s) contained no directives", cfg.Scenario.Name, cfg.Scenario.Path)
	}

	tracer, closeTrace, err := newNDJSONLogger(cfg.TraceOut, cfg.Scenario.Name)
	if err != nil {
		return fmt.Errorf("open trace: %w", err)
	}
	defer func() {
		_ = closeTrace()
	}()

	// Track if we've written config yet (write on first node creation)
	configWritten := false
	ndjsonTracer := tracer.(*ndjsonLogger)

	env := rafttest.NewInteractionEnv(&rafttest.InteractionOpts{
		OnConfig: func(raftCfg *raft.Config) {
			raftCfg.TraceLogger = tracer
			// Write config on first node creation
			if !configWritten {
				ndjsonTracer.WriteConfig(raftCfg.MaxInflightMsgs, raftCfg.DisableConfChangeValidation)
				configWritten = true
			}
		},
		SetRandomizedElectionTimeout: func(node *raft.RawNode, timeout int) {
			if err := setRandomizedElectionTimeout(node, timeout); err != nil {
				log.Printf("set randomized election timeout: %v", err)
			}
		},
		// Pass TraceLogger for manually injected messages (e.g., send-snapshot)
		TraceLogger: tracer,
	})

	stopRuntimeTrace, err := startRuntimeTrace(cfg.RuntimeTrace)
	if err != nil {
		return err
	}
	if stopRuntimeTrace != nil {
		defer func() {
			_ = stopRuntimeTrace()
		}()
	}

	if !raft.StateTraceDeployed {
		log.Printf("warning: state trace hooks disabled (build without -tags=with_tla); NDJSON will be empty")
	}

	var tb testing.T
	start := time.Now()
	if err := runDirectives(&tb, env, directives, cfg.Verbose, cfg.SkipValidation); err != nil {
		return err
	}
	duration := time.Since(start)

	if err := writeMetadata(cfg.MetadataOut, HarnessMetadata{
		Scenario:     cfg.Scenario.Name,
		ScenarioPath: cfg.Scenario.Path,
		TraceOut:     cfg.TraceOut,
		RuntimeTrace: cfg.RuntimeTrace,
		GoVersion:    runtime.Version(),
		BuildTags:    []string{"with_tla"},
		DurationMS:   duration.Milliseconds(),
		GitSHA:       currentGitSHA(),
		GeneratedAt:  time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	fmt.Printf("Scenario %q complete in %s\n", cfg.Scenario.Name, duration.Round(time.Millisecond))
	fmt.Printf("State trace: %s\n", cfg.TraceOut)
	if cfg.RuntimeTrace != "" {
		fmt.Printf("Runtime trace: %s\n", cfg.RuntimeTrace)
	}
	fmt.Printf("Metadata: %s\n", cfg.MetadataOut)
	return nil
}

func runDirectives(t *testing.T, env *rafttest.InteractionEnv, directives []directive, verbose bool, skipValidate bool) error {
	for _, td := range directives {
		output := env.Handle(t, td.TestData)
		if verbose && strings.TrimSpace(output) != "" {
			fmt.Printf("=== %s (%s)\n%s\n", td.Cmd, td.Pos, output)
		}

		if skipValidate || td.Expected == "" {
			continue
		}
		if strings.TrimSpace(td.Expected) != strings.TrimSpace(output) {
			return fmt.Errorf("output mismatch at %s\nexpected:\n%s\nactual:\n%s\n", td.Pos, td.Expected, output)
		}
	}
	return nil
}

func startRuntimeTrace(path string) (func() error, error) {
	if path == "" {
		return nil, nil
	}

	if err := mkdirAll(path); err != nil {
		return nil, fmt.Errorf("prepare runtime trace path: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create runtime trace: %w", err)
	}

	if err := rt.Start(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start runtime trace: %w", err)
	}

	return func() error {
		rt.Stop()
		return f.Close()
	}, nil
}

func resolveScenario(s string) (scenarioResolution, error) {
	if s == "" {
		return scenarioResolution{}, errors.New("scenario name or path is required")
	}

	if fi, err := os.Stat(s); err == nil && !fi.IsDir() {
		return scenarioResolution{Name: strings.TrimSuffix(filepath.Base(s), filepath.Ext(s)), Path: s}, nil
	}

	if path, ok := scenarioMap()[s]; ok {
		return scenarioResolution{Name: s, Path: path}, nil
	}

	alt := filepath.Join("testdata", s)
	if fi, err := os.Stat(alt); err == nil && !fi.IsDir() {
		return scenarioResolution{Name: strings.TrimSuffix(s, filepath.Ext(s)), Path: alt}, nil
	}

	return scenarioResolution{}, fmt.Errorf("unknown scenario %q (provide a path or use -list)", s)
}

func listScenarios() error {
	fmt.Println("Built-in scenarios:")
	for name, path := range scenarioMap() {
		fmt.Printf("  %s -> %s\n", name, path)
	}
	return nil
}

func scenarioMap() map[string]string {
	return map[string]string{
		"basic":                                 filepath.Join("testdata", "campaign.txt"),
		"basic_election":                        filepath.Join("testdata", "campaign.txt"),
		"confchange_add_remove":                 filepath.Join("testdata", "confchange_v2_add_single_auto.txt"),
		"leader_transfer":                       filepath.Join("testdata", "confchange_v2_replace_leader.txt"),
		"async_storage_writes":                  filepath.Join("testdata", "async_storage_writes.txt"),
		"snapshot_and_recovery":                 filepath.Join("testdata", "snapshot_succeed_via_app_resp.txt"),
		"partition_and_recover":                 filepath.Join("testdata", "replicate_pause.txt"),
		"forget_leader":                         filepath.Join("testdata", "forget_leader.txt"),
		"async_storage_writes_append_aba_race":  filepath.Join("testdata", "async_storage_writes_append_aba_race.txt"),
		"campaign_learner_must_vote":            filepath.Join("testdata", "campaign_learner_must_vote.txt"),
		"checkquorum":                           filepath.Join("testdata", "checkquorum.txt"),
		"confchange_disable_validation":         filepath.Join("testdata", "confchange_disable_validation.txt"),
		"confchange_v1_add_single":              filepath.Join("testdata", "confchange_v1_add_single.txt"),
		"confchange_v1_remove_leader_stepdown":  filepath.Join("testdata", "confchange_v1_remove_leader_stepdown.txt"),
		"confchange_v1_remove_leader":           filepath.Join("testdata", "confchange_v1_remove_leader.txt"),
		"confchange_v2_add_double_auto":         filepath.Join("testdata", "confchange_v2_add_double_auto.txt"),
		"confchange_v2_add_double_implicit":     filepath.Join("testdata", "confchange_v2_add_double_implicit.txt"),
		"confchange_v2_add_single_explicit":     filepath.Join("testdata", "confchange_v2_add_single_explicit.txt"),
		"confchange_v2_replace_leader_stepdown": filepath.Join("testdata", "confchange_v2_replace_leader_stepdown.txt"),
		"forget_leader_prevote_checkquorum":     filepath.Join("testdata", "forget_leader_prevote_checkquorum.txt"),
		"forget_leader_read_only_lease_based":   filepath.Join("testdata", "forget_leader_read_only_lease_based.txt"),
		"heartbeat_resp_recovers_from_probing":  filepath.Join("testdata", "heartbeat_resp_recovers_from_probing.txt"),
		"lagging_commit":                        filepath.Join("testdata", "lagging_commit.txt"),
		"prevote_checkquorum":                   filepath.Join("testdata", "prevote_checkquorum.txt"),
		"prevote":                               filepath.Join("testdata", "prevote.txt"),
		"probe_and_replicate":                   filepath.Join("testdata", "probe_and_replicate.txt"),
		"single_node":                           filepath.Join("testdata", "single_node.txt"),
		"slow_follower_after_compaction":        filepath.Join("testdata", "slow_follower_after_compaction.txt"),
		"snapshot_succeed_via_app_resp_behind":  filepath.Join("testdata", "snapshot_succeed_via_app_resp_behind.txt"),
		"snapshot_status_report":                filepath.Join("testdata", "snapshot_status_report.txt"),
		"snapshot_status_report_failure":        filepath.Join("testdata", "snapshot_status_report_failure.txt"),
		"report_unreachable":                    filepath.Join("testdata", "report_unreachable.txt"),
	}
}
