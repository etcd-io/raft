package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/datadriven"

	"go.etcd.io/raft/v3"
)

type directive struct {
	datadriven.TestData
}

type lineScanner struct {
	*bufio.Scanner
	line int
}

func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

func (l *lineScanner) Err() error {
	return l.Scanner.Err()
}

func loadDirectives(path string) ([]directive, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()

	sc := &lineScanner{Scanner: bufio.NewScanner(f)}
	var directives []directive
	for sc.Scan() {
		line := sc.Text()
		pos := fmt.Sprintf("%s:%d", path, sc.line)
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		for strings.HasSuffix(trimmed, `\`) {
			if !sc.Scan() {
				return nil, fmt.Errorf("%s: line continuation missing following line", pos)
			}
			next := strings.TrimSpace(sc.Text())
			trimmed = strings.TrimSuffix(trimmed, `\`) + " " + next
		}

		cmd, args, err := datadriven.ParseLine(trimmed)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", pos, err)
		}
		if cmd == "" {
			continue
		}

		td := datadriven.TestData{
			Pos:     pos,
			Cmd:     cmd,
			CmdArgs: args,
		}

		var buf strings.Builder
		var separator bool
		for sc.Scan() {
			text := sc.Text()
			if text == "----" {
				separator = true
				break
			}
			fmt.Fprintln(&buf, text)
		}
		td.Input = strings.TrimSpace(buf.String())
		if separator {
			td.Expected = readExpected(sc)
		}
		directives = append(directives, directive{TestData: td})
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return directives, nil
}

func readExpected(sc *lineScanner) string {
	var buf strings.Builder
	var line string
	allowBlankLines := false

	if sc.Scan() {
		line = sc.Text()
		if line == "----" {
			allowBlankLines = true
		}
	} else {
		return ""
	}

	if allowBlankLines {
		for sc.Scan() {
			line = sc.Text()
			if line == "----" {
				if sc.Scan() {
					next := sc.Text()
					if next == "----" {
						// Optional blank line after the double separator.
						if sc.Scan() {
							if strings.TrimSpace(sc.Text()) != "" {
								fmt.Fprintln(&buf, sc.Text())
							}
						}
						break
					}
					fmt.Fprintln(&buf, line)
					line = next
				}
			}
			fmt.Fprintln(&buf, line)
		}
	} else {
		for {
			if strings.TrimSpace(line) == "" {
				break
			}
			fmt.Fprintln(&buf, line)
			if !sc.Scan() {
				break
			}
			line = sc.Text()
		}
	}

	return buf.String()
}

type ndjsonLogger struct {
	scenario string
	enc      *json.Encoder
	closer   *os.File
	mu       sync.Mutex
}

type traceLine struct {
	Timestamp time.Time          `json:"ts"`
	Scenario  string             `json:"scenario"`
	Tag       string             `json:"tag"`
	Event     *raft.TracingEvent `json:"event,omitempty"`
	Config    *traceConfig       `json:"config,omitempty"`
}

type traceConfig struct {
	MaxInflightMsgs             int  `json:"MaxInflightMsgs"`
	DisableConfChangeValidation bool `json:"DisableConfChangeValidation,omitempty"`
}

func newNDJSONLogger(path, scenario string) (raft.TraceLogger, func() error, error) {
	if err := mkdirAll(path); err != nil {
		return nil, nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}

	l := &ndjsonLogger{
		scenario: scenario,
		enc:      json.NewEncoder(f),
		closer:   f,
	}
	return l, l.Close, nil
}

func (l *ndjsonLogger) TraceEvent(evt *raft.TracingEvent) {
	if evt == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enc.Encode(traceLine{
		Timestamp: time.Now().UTC(),
		Scenario:  l.scenario,
		Tag:       "trace",
		Event:     evt,
	}); err != nil {
		log.Printf("state trace encode error: %v", err)
	}
}

func (l *ndjsonLogger) WriteConfig(maxInflightMsgs int, disableConfChangeValidation bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enc.Encode(traceLine{
		Timestamp: time.Now().UTC(),
		Scenario:  l.scenario,
		Tag:       "config",
		Config: &traceConfig{
			MaxInflightMsgs:             maxInflightMsgs,
			DisableConfChangeValidation: disableConfChangeValidation,
		},
	}); err != nil {
		log.Printf("config encode error: %v", err)
	}
}

func (l *ndjsonLogger) Close() error {
	if l.closer == nil {
		return nil
	}
	return l.closer.Close()
}

type HarnessMetadata struct {
	Scenario     string    `json:"scenario"`
	ScenarioPath string    `json:"scenario_path"`
	TraceOut     string    `json:"trace_out"`
	RuntimeTrace string    `json:"runtime_trace,omitempty"`
	GoVersion    string    `json:"go_version"`
	BuildTags    []string  `json:"build_tags,omitempty"`
	DurationMS   int64     `json:"duration_ms"`
	GitSHA       string    `json:"git_sha,omitempty"`
	GeneratedAt  time.Time `json:"generated_at"`
}

func writeMetadata(path string, meta HarnessMetadata) error {
	if err := mkdirAll(path); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(meta)
}

func mkdirAll(path string) error {
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func currentGitSHA() string {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
