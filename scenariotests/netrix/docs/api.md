# Netrix DSL — API Reference

Package `netrix` (`go.etcd.io/raft/v3/scenariotests/netrix`) implements the
testing DSL described in:

> *A Domain Specific Language for Testing Consensus Implementations*  
> Dragoi, Enea, Nagendra, Srivas (arXiv:2303.05893, 2023)

It builds on `rafttest.InteractionEnv` and provides four composable
abstractions—**Condition**, **Action**, **Filter**, and **StateMachine**—plus a
**Runner** that drives them against a live in-process Raft cluster.

---

## Core Types

### `Event`

Represents a single observable step in test execution.

```go
type Event struct {
    ID   string        // unique within a run (e.g. "e1", "e2")
    From uint64        // sending node ID (0 if not applicable)
    To   uint64        // receiving node ID (0 if not applicable)
    Msg  *pb.Message   // raft message (nil for non-message events)
    Type EventType     // MessageSendEventType | MessageReceiveEventType
}
```

Events flow in pairs: a **send** event is created when a message enters the
in-flight pool; a **receive** event is synthesized after the message is
delivered to its destination node.

### `Context`

Shared mutable state available to every Condition and Action invocation within
a single test run.

```go
type Context struct {
    MessagePool map[string]*pb.Message  // all in-flight messages, keyed by event ID
    VarSet      map[string]any          // generic key-value store for inter-filter state
}
```

`Counter` and `MessageSet` use `VarSet` internally; you can also read/write it
directly if you need custom state.

---

## Condition

```go
type Condition func(*Event, *Context) bool
```

Conditions are plain functions composed with method chaining.

### Combinators

| Expression | Meaning |
|---|---|
| `c.And(other)` | both `c` and `other` are true |
| `c.Or(other)` | either `c` or `other` is true |
| `c.Not()` | negation of `c` |

### Built-in Conditions

| Function | True when… |
|---|---|
| `IsMessageSend()` | event is an outbound message (send phase) |
| `IsMessageReceive()` | event is an inbound message (receive phase) |
| `IsMessageType(t pb.MessageType)` | message type matches `t` |
| `IsMessageFrom(id uint64)` | message `From` field equals `id` |
| `IsMessageTo(id uint64)` | message `To` field equals `id` |
| `Always()` | always true (pass-through) |
| `Never()` | always false (block-all) |
| `When(f func() bool)` | evaluates `f()` at call time; useful for closing over external state |

---

## Action

```go
type Action func(*Event, *Context) []*pb.Message
```

Actions decide which messages to deliver as a consequence of an event. Returning
nil or an empty slice means the triggering message is dropped.

### Built-in Actions

| Function | Effect |
|---|---|
| `DeliverMessage()` | deliver the current event's message to its recipient |
| `DropMessage()` | suppress delivery (return nothing) |
| `RecordMessageAs(label string)` | store the message in `Context.VarSet[label]` **and** deliver it |

Multiple actions passed to `If().Then(...)` are composed: their outputs are
unioned and deduplicated by pointer identity.

---

## Counter

A named integer stored in `Context.VarSet`, initialized to 0.

```go
ctr := netrix.Count("label")   // returns Counter
```

### Counter Actions

| Expression | Effect |
|---|---|
| `ctr.Incr()` | increment counter by 1; delivers the current message unchanged |

### Counter Conditions

| Expression | True when… |
|---|---|
| `ctr.Lt(n)` | counter < n |
| `ctr.Leq(n)` | counter ≤ n |
| `ctr.Gt(n)` | counter > n |
| `ctr.Geq(n)` | counter ≥ n |
| `ctr.LtF(f)` | counter < `f(event, ctx)` at evaluation time |
| `ctr.GtF(f)` | counter > `f(event, ctx)` at evaluation time |

---

## MessageSet

A named collection of `*pb.Message` stored in `Context.VarSet`.

```go
s := netrix.Set("label")   // returns MessageSet
```

### MessageSet Actions

| Expression | Effect |
|---|---|
| `s.Store()` | append message to the set; **suppress** delivery |
| `s.DeliverAll()` | deliver all stored messages and clear the set; ignores the current event's message |

Combine `Store()` and `DeliverAll()` to buffer messages and replay them later:

```go
// Buffer the first two MsgVote messages...
filters.AddFilter(netrix.If(
    netrix.IsMessageType(pb.MsgVote).And(netrix.Count("n").Lt(2)),
).Then(netrix.Count("n").Incr(), netrix.Set("votes").Store()))

// ...then flush them on the third.
filters.AddFilter(netrix.If(
    netrix.IsMessageType(pb.MsgVote).And(netrix.Count("n").Geq(2)),
).Then(netrix.Set("votes").DeliverAll(), netrix.DeliverMessage()))
```

### MessageSet Conditions

| Expression | True when… |
|---|---|
| `s.Contains()` | the current event's message is in the set (pointer equality) |
| `s.Len(pred func(int) bool)` | `pred(len(set))` is true |

---

## Filter / FilterSet

A **FilterFunc** handles one event and signals whether it matched:

```go
type FilterFunc func(*Event, *Context) ([]*pb.Message, bool)
// bool == true → this filter handled the event; stop trying further filters
// bool == false → pass to next filter
```

The **`If().Then()`** builder constructs a FilterFunc from a Condition and one
or more Actions:

```go
netrix.If(cond).Then(action1, action2, ...)
```

A **FilterSet** is an ordered list of FilterFuncs with a built-in
deliver-all fallback.

```go
filters := netrix.NewFilterSet()
filters.AddFilter(netrix.If(netrix.IsMessageType(pb.MsgVote)).Then(netrix.DropMessage()))
```

Filters are evaluated in insertion order; the first that returns `handled=true`
wins. If no filter matches, the default filter delivers the message.

**Custom default:**

```go
filters := netrix.NewFilterSetWithDefault(myDefaultFilterFunc)
```

---

## Partition

Models a network partition between node groups. The partition state (`active`
bool) can be toggled imperatively or via Actions.

### Constructors

| Function | Drops messages… |
|---|---|
| `IsolateNode(nodeID)` | to or from `nodeID` |
| `IsolateNodes(ids...)` | to or from any node in `ids` |
| `PartitionSides(sideA, sideB)` | crossing between the two groups |

### Methods

| Method | Description |
|---|---|
| `p.Isolate()` | activate partition (call from `SetupFunc` / `TickFunc`) |
| `p.Heal()` | deactivate partition |
| `p.IsolateAction()` | Action that activates the partition and passes the current message |
| `p.HealAction()` | Action that deactivates the partition and passes the current message |
| `p.Active() bool` | raw bool; use when composing with other external state |
| `p.IsActive()` | Condition: true while partition is active |
| `p.IsHealed()` | Condition: true after partition is healed |
| `p.Filter()` | FilterFunc: drops crossing messages while active; register once with `AddFilter` |

**Typical pattern:**

```go
part := netrix.IsolateNode(1)

// Register the drop rule.
filters.AddFilter(part.Filter())

// Activate on some trigger.
filters.AddFilter(netrix.If(triggerCond).Then(part.IsolateAction()))

// Use in state machine.
sm.Builder().On(part.IsHealed(), netrix.SuccessState)
```

---

## StateMachine

A deterministic finite automaton over the event stream. Transitions are
evaluated in insertion order; the first matching transition fires.

```go
sm  := netrix.NewStateMachine()
init := sm.Builder()                               // StateBuilder at initial state

init.On(cond1, "state-a").                         // initial → state-a
     On(cond2, netrix.SuccessState)               // state-a → success
```

### Sentinel States

| Constant | Meaning |
|---|---|
| `SuccessState` (`"__success__"`) | the scenario property was satisfied |
| `FailureState` (`"__failure__"`) | the scenario property was violated |

### `StateBuilder` methods

| Method | Description |
|---|---|
| `sb.On(cond, target)` | add a transition; returns a `StateBuilder` rooted at `target` |
| `sb.MarkSuccess()` | mark the current state as a success state |

### `StateMachine` methods

| Method | Description |
|---|---|
| `sm.Step(e, ctx)` | advance by one event (called automatically by `Run`) |
| `sm.IsSuccess()` | true when in a success state |
| `sm.IsFailure()` | true when in the failure state |
| `sm.IsTerminal()` | `IsSuccess() || IsFailure()` |
| `sm.CurrentState()` | label of current state |
| `sm.Reset()` | return to initial state (used between `Iterations`) |

---

## TestCase

Bundles all configuration for one test scenario.

```go
type TestCase struct {
    Name       string
    MaxRounds  int                                       // 0 = unlimited
    Iterations int                                       // 0 or 1 = single run
    EnvFunc    func() *rafttest.InteractionEnv           // required when Iterations > 1
    StateMachine *StateMachine                           // nil = no property assertion
    Filters      *FilterSet                              // nil = deliver all
    SetupFunc  func(*rafttest.InteractionEnv) error      // called before the run loop
    TickFunc   func(*rafttest.InteractionEnv, round int) // called once per round
}
```

`TickFunc` is the mechanism for driving election and heartbeat timeouts; call
`env.Tick(nodeIndex, n)` inside it.

---

## Run / RunResult

```go
func Run(tc *TestCase, env *rafttest.InteractionEnv) RunResult
```

Executes the `TestCase`. When `tc.Iterations > 1`, `env` is ignored; `tc.EnvFunc`
is called fresh each iteration and the state machine is reset between attempts.
The run succeeds as soon as any iteration succeeds.

```go
type RunResult struct {
    Success    bool
    FinalState string   // state machine label, or "no-state-machine"
    Rounds     int
    Iteration  int      // 1-based; 0 when Iterations <= 1
    Events     []*Event
    Err        error
}

func (r RunResult) IsFailure() bool
```

---

## Round loop

Each round the runner:

1. Calls `TickFunc` (if set).
2. Drains all pending `Ready` states from every node.
3. Drains `AppendWork` and `ApplyWork` threads for every node.
4. Intercepts `env.Messages` and, for each message:
   a. Creates a **send** `Event`.
   b. Applies the `FilterSet` to determine which messages to deliver.
   c. Steps the `StateMachine` on the send event; stops if terminal.
   d. Delivers the decided messages.
   e. Creates a **receive** `Event` for each delivered message.
   f. Steps the `StateMachine` on each receive event; stops if terminal.
5. If no work was done in the entire round (fixed point), stops.
