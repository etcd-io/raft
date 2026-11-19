# System-Level TLA+ Specification for etcd/raft

This directory contains a system-level TLA+ specification for the etcd/raft library, complementing the protocol-level specification in `../`. The specification models implementation details including progress tracking (`StateProbe`, `StateReplicate`, `StateSnapshot`), flow control (`MsgAppFlowPaused`, Inflights), detailed snapshot handling, and joint consensus configuration changes.

## Model Checking (Simulation)

```bash
java -XX:+UseParallelGC \
    -cp tla2tools.jar:CommunityModules-deps.jar \
    tlc2.TLC \
    -config MCetcdraft.cfg \
    MCetcdraft.tla \
    -simulate \
    -depth 100
```

## Invariants

The specification defines **88 invariants** derived from three sources:
- **Raft Paper:** Safety properties
- **Code:** Implementation details
- **Issue/Bug:** Historical bug regression tests
