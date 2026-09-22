# Netrix DSL — Overview

## Background

The Netrix DSL is described in:

> *A Domain Specific Language for Testing Consensus Implementations*  
> Dragoi, Enea, Nagendra, Srivas (arXiv:2303.05893, 2023)

The key idea is to express network testing scenarios as composable rules over
the message stream rather than writing ad-hoc interleaving loops. This
implementation embeds the DSL directly inside the `rafttest` package, so tests
run entirely in-process without an external controller.

---

## Conceptual model

A Netrix test scenario has three parts that work together:

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

The **Runner** drives the cluster round by round, intercepting messages,
applying filters, and stepping the state machine until it reaches a terminal
state or the round limit is hit.

---

## Abstractions

### Condition

A `Condition` is a predicate `func(*Event, *Context) bool`. Built-ins cover
common cases (`IsMessageType`, `IsMessageFrom`, `IsMessageTo`, `Always`, …).
Conditions compose with `.And()`, `.Or()`, `.Not()`.

```go
// Drop only heartbeat responses from node 1 to node 2.
netrix.IsMessageType(pb.MsgHeartbeatResp).
    And(netrix.IsMessageFrom(1)).
    And(netrix.IsMessageTo(2))
```

### Action

An `Action` is `func(*Event, *Context) []*pb.Message`. It returns the messages
that should be forwarded. Built-ins are `DeliverMessage()`, `DropMessage()`, and
`RecordMessageAs(label)`.

### Filter

The `If(cond).Then(actions…)` builder glues a condition to one or more actions.
Filters are collected in a `FilterSet` and evaluated in order. The first
matching filter handles the event; unmatched events fall through to the
deliver-all default.

### Counter / MessageSet

Higher-level helpers stored in `Context.VarSet`:

- **`Count(label)`** — named integer; `.Incr()` action, `.Lt/.Gt/.Leq/.Geq`
  conditions.
- **`Set(label)`** — named message buffer; `.Store()` suppresses delivery and
  buffers; `.DeliverAll()` flushes the buffer later.

### Partition

`Partition` models a network partition between node groups. It exposes:

- Imperative methods (`Isolate()`, `Heal()`) for use in `SetupFunc`/`TickFunc`.
- Actions (`IsolateAction()`, `HealAction()`) that can be composed into filters.
- Conditions (`IsActive()`, `IsHealed()`) for state machine transitions.
- A drop FilterFunc (`Filter()`) to register once with `FilterSet.AddFilter`.

### StateMachine

A deterministic finite automaton over the event stream. Build it with a fluent
`On(cond, "target")` chain. Sentinel targets `SuccessState` and `FailureState`
terminate the run immediately.

```go
sm   := netrix.NewStateMachine()
init := sm.Builder()
// Succeed once we observe a MsgApp; fail immediately if we see a MsgSnap.
step1 := init.On(netrix.IsMessageType(pb.MsgApp), "got-leader")
step1.On(netrix.IsMessageType(pb.MsgSnap), netrix.FailureState)
step1.On(netrix.IsMessageType(pb.MsgApp).And(...), netrix.SuccessState)
```

---

## Writing a test

```go
func TestMyScenario(t *testing.T) {
    env := newEnv(t) // create 3-node cluster

    // 1. Build the state machine (property to assert).
    sm   := netrix.NewStateMachine()
    init := sm.Builder()
    init.On(netrix.IsMessageType(pb.MsgApp), netrix.SuccessState)

    // 2. Build the filter set (message routing policy).
    filters := netrix.NewFilterSet()
    filters.AddFilter(
        netrix.If(netrix.IsMessageType(pb.MsgVote)).Then(netrix.DropMessage()),
    )

    // 3. Define the test case.
    tc := &netrix.TestCase{
        Name:         "my-scenario",
        MaxRounds:    50,
        StateMachine: sm,
        Filters:      filters,
        SetupFunc: func(e *rafttest.InteractionEnv) error {
            return e.Campaign(0)
        },
    }

    // 4. Run and assert.
    result := netrix.Run(tc, env)
    require.NoError(t, result.Err)
    require.True(t, result.Success)
}
```

---

## Iteration (non-deterministic scenarios)

When the scenario outcome depends on timing or ordering, use `Iterations` to
retry from a fresh environment until success:

```go
tc := &netrix.TestCase{
    Name:       "flaky-scenario",
    Iterations: 10,
    EnvFunc:    func() *rafttest.InteractionEnv { return newEnv(t) },
    // ... rest of config
}
result := netrix.Run(tc, nil) // env ignored; EnvFunc is called instead
```

The state machine is reset between iterations. The run stops and returns success
as soon as one iteration succeeds.

---

## Network partitions

```go
part := netrix.IsolateNode(1)  // or IsolateNodes / PartitionSides

// Register the drop rule — evaluated every round while the partition is active.
filters.AddFilter(part.Filter())

// Activate when some condition is met.
filters.AddFilter(netrix.If(triggerCond).Then(part.IsolateAction()))

// Assert the cluster recovers after healing.
sm.Builder().
    On(netrix.IsMessageType(pb.MsgApp), "leader-elected").
    On(part.IsHealed(), netrix.SuccessState)
```

Heal from `TickFunc` to simulate a time-bounded partition:

```go
TickFunc: func(env *rafttest.InteractionEnv, round int) {
    if round == 20 {
        part.Heal()
    }
    tickAll(env)
},
```

---

## Relationship to rafttest

The runner drives `rafttest.InteractionEnv` directly:

- `env.ProcessReady(i)` — commit one node's Ready batch.
- `env.ProcessAppendThread(i)` / `env.ProcessApplyThread(i)` — drain async work.
- `env.Messages` — the in-flight message pool; the runner intercepts and clears
  this each round, routing messages through the FilterSet before calling
  `env.Nodes[i].Step(msg)`.

No external process, no TCP, no goroutines beyond what rafttest already uses.
