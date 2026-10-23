# TLA+ Specification and Trace Validation for Raft Library: A Brief Guide

This document presents a brief guideline on how to validate the correctness of Raft library implemented in `etcd-io/raft`. A new TLA+ specification models the core algorithm of the library, including the distinctive behaviors like membership reconfiguration, that differentiate it from the classic Raft algorithm. The TLA+ specification serves dual purposes: it not only helps to verify the correctness of the model but also facilitates model-based trace validation.  

## Checking the Model with TLA+ Specifications

The TLA+ specification can be verified using the TLC model checker. Here are a few methods:

1. **TLA+ Toolbox**: This is an ideal tool for an in-depth study of the specification. For more information, refer to the [TLA+ Toolbox guideline](https://lamport.azurewebsites.net/tla/toolbox.html).

2. **VSCode Plugin TLA+ Nightly**: This is a viable alternative to the TLA+ Toolbox, particularly for those accustomed to using VSCode. For more information, refer to the [VSCode Plugin TLA+ Nightly guideline](https://github.com/tlaplus/vscode-tlaplus/wiki).

3. **CLI**: This is the best option for integration with existing test frameworks and automation. For more information, refer to the [CLI guideline](https://learntla.com/topics/cli.html). You can execute a typical command line to verify etcdraft.tla as shown below. Please ensure that tla2tools.jar and CommunityModules-deps.jar have been downloaded and are available in the current folder before proceeding.

    ```console
    java -XX:+UseParallelGC -cp tla2tools.jar:CommunityModules-deps.jar tlc2.TLC -config MCetcdraft.cfg MCetcdraft.tla -lncheck final -fpmem 0.9
    ```

## TLA+ Model-Based Trace Validation

The correctness of applications that implements a consensus algorithm is ensured by two factors: the correctness of the algorithm itself, and adherence of the implementation to algorithm specification.

The first factor, the correctness of the algorithm, is assured through model checking the specification. The second one, adherence of the implementation to algorithm specification, is fortified through trace validation, which serves to bridge the gap between the model and its implementation.

To ensure the robustness of trace validation, we initially inject multiple trace log points at specific sections in the code where state transitions occur (for example, SendAppendEntries, BecomeLeader, etc.). The trace validation specification leverages these traces as a state space constraint, guiding the state machine to traverse in a manner identical to that of the service.

If a trace suggests a state or transition that the state machine can't accommodate, it indicates a discrepancy between the model and its implementation. In cases where the model has already been verified by the TLC model checker, it's more likely that any issues arise from the implementation rather than the model. In other cases where the code change reflects expected refactoring, we need to update the model accordingly and model checking it before validating traces with the new model.

## Enable TLA+ Trace Validation

To activate trace collection, build the application using the "with_tla" tag (`go build -tags=with_tla`). The `StartNode` and/or `RestartNode` should be invoked with `Config` with `TraceLogger` property set to an instance of `TraceLogger` implemented in the application. 

`Traceetcdraft.tla` expects a trace file in NDJSON format. Each entry must include an `event` field containing a `TracingEvent` object. Here's an example trace logger using zap.Logger. Note that sampling shall be disabled to ensure all traces are logged (see [Zap FAQ: Why sample application logs?](https://github.com/uber-go/zap/blob/master/FAQ.md#why-sample-application-logs)).

```go
type TraceLogger interface {
	TraceEvent(*TracingEvent)
}

type MyTraceLogger struct{
  lg *zap.Logger
}

func (t *MyTraceLogger) TraceEvent(ev *TracingEvent) {
  t.lg.Debug("trace", zap.String("tag", "trace"), zap.Any("event", ev))
}

```

To start a three-node cluster with trace logger
```go
storage := raft.NewMemoryStorage()
  c := &raft.Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
    MaxSizePerMsg:   4096,
    MaxInflightMsgs: 256,
    TraceLogger:     &MyTraceLogger{},
  }
  // Set peer list to the other nodes in the cluster.
  // Note that they need to be started separately as well.
  n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})
```
**Note:** To preserve the causality of events across nodes, run all application instances on the same machine and store traces in the same file. This approach maintains the order of traces in all instances.

## Model Checking TLA+ Spec
The TLA+ spec defines the desired behaviors of the model. To validate the correctness of the model, we need to make sure following commands run enough time (at least hours, depending on how confident we need) without failure.

```console
./validate-model.sh -s ./MCetcdraft.tla -c ./MCetcdraft.cfg 
```

You can also add `-m` option to run model checking in simulation mode, which will randomly walk in the state machine and is helpful for finding issues quickly.

```console
./validate-model.sh -m -s ./MCetcdraft.tla -c ./MCetcdraft.cfg 
```

## Validate Collected Traces
With above example trace logger, validate.sh can be used to validate traces parallelly.

```console
./validate.sh -s ./Traceetcdraft.tla -c ./Traceetcdraft.cfg /path/to/traces/*.ndjson
```
By default validate.sh uses all CPU cores to validate the traces. You can also specify the maximum concurrent tasks by `-p` argument. 

Validating all traces in a file may take a long time. You can specify `MAX_TRACE` environment variable to limit the validation only on top `MAX_TRACE` traces in each file.

## Known Issues
1. **Partially persisted log**. 
etcdraft assumes atomic persisiting of states in Read action. This, however, may not apply in real-world application. For example, etcd may persist a prefix log when it crashes in the middle of saving data to disks. 
We will introduce non-deterministic validation method in future to address this issue.

2. **Long validation time**.
As aforementioned, trace validation may take long time when there are thousands of traces in a file. It typically takes a few minutes to validate 3000 traces. And the time increases non-linearly after that. 
The workaround for this issue is to run validation in batches after all traces are collected. Each file may take long time but the overall validation time can be reasonable in this way.
All means to reduce trace amount would help to reduce validation time, e.g. lower QPS and larger heartbeat timeout.