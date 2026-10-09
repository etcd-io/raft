# Netrix DSL

Package `netrix` (`go.etcd.io/raft/v3/scenariotests/netrix`) is a Go
implementation of the testing DSL introduced in:

> **A Domain Specific Language for Testing Consensus Implementations**  
> Cezara Dragoi, Constantin Enea, Srinidhi Nagendra, Mandayam Srivas  
> [arXiv:2303.05893](https://arxiv.org/abs/2303.05893) · NETYS 2024, Springer LNCS 14783

The original Netrix system ([netrixframework/netrix](https://github.com/netrixframework/netrix))
couples consensus implementations to an external controller over TCP and uses
that controller to intercept and schedule messages. This package embeds the same
four DSL abstractions—**Condition**, **Action**, **Filter**, and
**StateMachine**—directly inside the `rafttest.InteractionEnv`, so tests run
entirely in-process with no external dependencies.

---

## Motivation

Ad-hoc distributed-systems tests typically do one of two things: they use a
fixed interleaving script that only exercises one path, or they write bespoke
interleaving loops that are hard to read, compose, or reuse. The Netrix DSL
trades both for a declarative style: a **FilterSet** describes the network
policy (which messages to deliver, drop, or buffer), and a **StateMachine**
describes the property to assert over the resulting event stream. The runner
drives the cluster round by round and checks the property automatically.

---

## Concepts

```
  ┌──────────────────────────────────────────────────────────────┐
  │  TestCase                                                    │
  │                                                              │
  │  SetupFunc ──► initialise cluster (campaign, propose, …)     │
  │                                                              │
  │  FilterSet ──► route each message:                           │
  │                  If(condition).Then(action, …)               │
  │                                                              │
  │  StateMachine ──► assert a property over the event stream    │
  │                    On(condition, "next-state")               │
  └──────────────────────────────────────────────────────────────┘
```

Each **round** the runner:

1. Calls `TickFunc` (to advance logical clocks for heartbeat / election timeouts).
2. Drains all pending `Ready` states and async append/apply work.
3. Intercepts `env.Messages` and, for each message: applies the `FilterSet`,
   steps the `StateMachine` on the send event, delivers the decided messages,
   and steps the `StateMachine` on each receive event.
4. Stops when the state machine reaches a terminal state, no work remains (fixed
   point), or `MaxRounds` is exceeded.

---

## Quick start

```go
func TestElectionSucceeds(t *testing.T) {
    env := newEnv(t)  // 3-node rafttest cluster

    // Assert: eventually a leader sends a no-op MsgApp.
    sm   := netrix.NewStateMachine()
    init := sm.Builder()
    init.On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState)

    result := netrix.Run(&netrix.TestCase{
        Name:         "election-succeeds",
        MaxRounds:    50,
        StateMachine: sm,
        TickFunc: func(env *rafttest.InteractionEnv, round int) {
            tickAll(env)
        },
    }, env)

    require.NoError(t, result.Err)
    require.True(t, result.Success)
}
```

---

## API summary

Full documentation is in [docs/api.md](docs/api.md). An overview of the
conceptual model with extended examples is in [docs/overview.md](docs/overview.md).

### Condition

A `Condition` is a predicate `func(*Event, *Context) bool`. Built-ins:

| Function | True when… |
|---|---|
| `IsMessageType(t)` | message type equals `t` |
| `IsMessageFrom(id)` | sender node ID equals `id` |
| `IsMessageTo(id)` | recipient node ID equals `id` |
| `IsMessageSend()` | send phase (before delivery) |
| `IsMessageReceive()` | receive phase (after delivery) |
| `Always()` / `Never()` | unconditional pass / block |
| `When(f func() bool)` | close over external state |

Compose with `.And()`, `.Or()`, `.Not()`.

### Action

An `Action` is `func(*Event, *Context) []*pb.Message`. Built-ins:

| Function | Effect |
|---|---|
| `DeliverMessage()` | forward the current message |
| `DropMessage()` | suppress delivery |
| `RecordMessageAs(label)` | store in `Context.VarSet` and deliver |

### Counter / MessageSet

Higher-level helpers stored in `Context.VarSet`:

- `Count("label")` — named integer; `.Incr()` action, `.Lt/.Gt/.Leq/.Geq` conditions.
- `Set("label")` — named message buffer; `.Store()` suppresses delivery, `.DeliverAll()` flushes later.

### Partition

`IsolateNode(id)` / `IsolateNodes(ids...)` / `PartitionSides(sideA, sideB)` model
network partitions. Each exposes `.Isolate()` / `.Heal()` (imperative),
`.IsolateAction()` / `.HealAction()` (filter actions), `.IsActive()` /
`.IsHealed()` (conditions), and `.Filter()` (a `FilterFunc` that drops crossing
messages while active).

### FilterSet

```go
filters := netrix.NewFilterSet()
filters.AddFilter(netrix.If(cond).Then(action1, action2))
```

Filters are evaluated in insertion order; the first match wins. Unmatched
messages are delivered by default.

### StateMachine

```go
sm   := netrix.NewStateMachine()
init := sm.Builder()
step := init.On(condA, "state-a")   // initial → state-a
step.On(condB, netrix.SuccessState)  // state-a → success
```

Sentinel targets: `netrix.SuccessState` and `netrix.FailureState`.

### Run

```go
result := netrix.Run(tc, env)
// result.Success, result.FinalState, result.Rounds, result.Events, result.Err
```

Set `tc.Iterations > 1` and provide `tc.EnvFunc` for non-deterministic scenarios;
the runner resets the state machine and rebuilds the environment between attempts,
stopping as soon as one iteration succeeds.

---

## Running the tests

```sh
go test go.etcd.io/raft/v3/scenariotests/netrix/...
go test go.etcd.io/raft/v3/scenariotests/...
```
