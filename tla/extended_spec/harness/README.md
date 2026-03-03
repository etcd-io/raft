# Trace Validation Harness

This harness generates execution traces from raft's datadriven test scenarios and validates them against the TLA+ specification.

## Prerequisites

- Go 1.23+
- Java (for TLC model checker)
- TLA+ tools: [tla2tools.jar](https://nightly.tlapl.us/dist/tla2tools.jar) and [CommunityModules-deps.jar](https://github.com/tlaplus/CommunityModules/releases/latest/download/CommunityModules-deps.jar)

## Step 1: Apply Instrumentation Patch

The patch adds trace logging hooks to the raft library, guarded by the `with_tla` build tag (zero overhead in production builds).

```bash
cd /path/to/raft
git apply tla/extended_spec/harness/patch/instrumentation.patch
```

## Step 2: Generate Traces

```bash
cd tla/extended_spec/harness
go build -tags=with_tla -o harness .

# Run a single scenario
./harness -scenario basic_election

# Run all scenarios
./harness -set all

# List available scenarios
./harness -list
```

Traces are written to `traces/<scenario>.ndjson`.

## Step 3: Validate Traces Against TLA+ Spec

Validate a single trace:

```bash
env JSON=traces/basic_election.ndjson \
  java -XX:+UseParallelGC \
    -cp tla2tools.jar:CommunityModules-deps.jar \
    tlc2.TLC -config ../Traceetcdraft.cfg ../Traceetcdraft.tla \
    -lncheck final
```

A successful validation ends with `Model checking completed. No error has been found.`

Validate all traces in batch using `validate.sh`:

```bash
./validate.sh \
  -s ../Traceetcdraft.tla \
  -c ../Traceetcdraft.cfg \
  traces/*.ndjson
```

`validate.sh` runs TLC in parallel (one process per trace) and reports PASS/FAIL for each trace. Usage:

```
validate.sh [-p <parallel>] -s <spec> -c <config> <trace files>
```

- `-s` — path to `Traceetcdraft.tla`
- `-c` — path to `Traceetcdraft.cfg`
- `-p` — number of parallel workers (default: `nproc`)

The script downloads `tla2tools.jar` and `CommunityModules-deps.jar` automatically if not found in `$TOOLDIR`.
