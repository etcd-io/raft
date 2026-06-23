
<hr>

## v3.7.0(2026-06-21)

### Changelog since v3.7.0-rc.1
There isn't any production code change since v3.7.0-rc.1. Only golang patch version is bumped.

<hr>

## v3.7.0-rc.1(2026-05-27)

### Bug Fixes

- [Update EnsureSnapshot and EnsureSnapshotMetadata to avoid race](https://github.com/etcd-io/raft/pull/442)

<hr>

## v3.7.0-rc.0(2026-05-27)

### Features
- [Allow users to pass in a snapshot with only the ConfState initialized during bootstrap](https://github.com/etcd-io/raft/pull/370)
- [Improve the ReadIndex flow to prevent stale read index caused by RequestIndex retries](https://github.com/etcd-io/raft/pull/397)
- [Migrate gogo/protobuf to standard protobuf](https://github.com/etcd-io/etcd/issues/14533)

### Minor changes
- [Align formatting of node ID in MajorityConfig](https://github.com/etcd-io/raft/pull/414)

