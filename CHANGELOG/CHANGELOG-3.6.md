Note that we start to track changes starting from v3.6.

<hr>

## v3.6.0-beta.0(2024-11-20)

### Changelog since v3.6.0-alpha.0
- [Minor refactoring `raft.maybeSendAppend`](https://github.com/etcd-io/raft/pull/136)
- [Add entryID and logSlice types](https://github.com/etcd-io/raft/pull/145)
- [Fix next index might be smaller than match index](https://github.com/etcd-io/raft/pull/149)
- [Minor refactoring raftLog initialization](https://github.com/etcd-io/raft/pull/151)
- [cleanup Match, Next and MaybeUpdate](https://github.com/etcd-io/raft/pull/165)
- [tracker: track in-flight commit index](https://github.com/etcd-io/raft/pull/171)
- [Replace sort.Slice with slices.Sort, slices.SortFunc](https://github.com/etcd-io/raft/pull/221)

### Others
- [Introduce TLA+ trace validation](https://github.com/etcd-io/raft/pull/113)

<hr>

## v3.6.0-alpha.0(2024-01-12)

### Features
- [Add MaxInflightBytes setting in `raft.Config` for better flow control of entries](https://github.com/etcd-io/etcd/pull/14624)
- [Send empty `MsgApp` when entry in-flight limits are exceeded](https://github.com/etcd-io/etcd/pull/14633)
- [Support asynchronous storage writes](https://github.com/etcd-io/raft/pull/8)
- [Paginate the unapplied config changes scan](https://github.com/etcd-io/raft/pull/32)
- [Add ForgetLeader](https://github.com/etcd-io/raft/pull/78)
- [Add StepDownOnRemoval](https://github.com/etcd-io/raft/pull/79)
- [Accept any snapshot that allows replication](https://github.com/etcd-io/raft/pull/110)

### Others
- [Deprecate RawNode.TickQuiesced()](https://github.com/etcd-io/raft/pull/62)
