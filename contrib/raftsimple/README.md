# raftsimple

`raftsimple` is a simplified, educational example of the Raft consensus algorithm built on top of `go.etcd.io/raft`.

<!-- While the official `raftexample` demonstrates how to integrate `etcd/raft` with production-grade networking (`rafthttp`) and storage (`wal`), `raftsimple` strips away these heavy dependencies. It runs an entire multi-node cluster within a single OS process, communicating via an in-memory network and writing to a lightweight, transparent Write-Ahead Log (WAL). -->

This project is designed specifically for developers who want to understand the pure mechanics of the Raft algorithm, the `Ready()` event loop, and state machine replication without getting bogged down in production infrastructure code.

## Getting Started

To compile and run the project, ensure you have Go installed, then build the binary:

```bash
go build -o raftsimple

```

By default, executing the binary will spin up a 3-node Raft cluster in a single process:

```bash
./raftsimple

```

You can easily specify a different number of starting nodes using the `-nodes` flag:

```bash
./raftsimple -nodes 5

```

## API Usage

Each Raft node exposes its own key-value store HTTP API at port `9120 + nodeID` (using one-based indexing). For example, Node 1 exposes port `9121`, Node 2 exposes port `9122`, and so on.

The API supports `GET`, `POST`, `PUT`, and `DELETE` methods to manage key-value pairs and cluster membership.

Open a separate terminal to execute the following commands. During execution, you can monitor the main terminal to see the Raft consensus algorithm in effect.

**Key-Value Operations:**

* **Create or Replace a Key:**
```bash
curl http://127.0.0.1:9121/foo -XPUT -d bar

```


*Adds or replaces the key `foo` with the value `bar` to the store.*
* **Read a Key:**
```bash
curl http://127.0.0.1:9121/foo

```


*Retrieves the value from key `foo`. Will throw `Failed to GET` if the key doesn't exist.*

**Cluster Membership and Lifecycle Operations:**

* **Add or Revive a Node:**
```bash
curl http://127.0.0.1:9121/4 -XPOST

```


*If Node 4 does not exist, it creates a new Raft node and its key-value API at port `9124`, then proposes a cluster membership change to add it. If Node 4 was previously stopped (crashed), it recovers its state from disk and restarts the node.*
* **Stop (Crash) a Node:**
```bash
curl http://127.0.0.1:9121/1 -XDELETE

```


*Simulates a crash by abruptly stopping Raft Node 1 and its key-value store API. This does NOT formally remove the node from the Raft cluster configuration; other nodes will still attempt to communicate with it.*
* **Revive a Stopped Node:**
```bash
curl http://127.0.0.1:9122/1 -XPOST

```


*(Note: We route this request through an active node, like Node 2 at port `9122`, because Node 1's API is down).*
*Restarts the crashed Raft Node 1. It reloads its snapshot and WAL from disk and re-enables its key-value store API at port `9121`.*



## Project Architecture

`raftsimple` is architected specifically for readability and local experimentation:

* **Single-Process Execution:** All Raft nodes run as concurrent goroutines within a single `./raftsimple` process. The `NodeManager` (defined in `nodemanager.go`) is responsible for orchestrating the setup and lifecycle of this in-process cluster, while `main.go` serves as the application entry point.
* **In-Memory Transport:** Nodes do not communicate over TCP/HTTP. Instead, they interact via a global, thread-safe `network` struct that tracks all running nodes by their memory addresses. This eliminates network latency and serialization overhead while ensuring safe concurrent access as nodes are dynamically added or removed.
* **Transparent Storage:** `raftsimple` implements a basic Write-Ahead Log. `HardState`, `Entries`, and `Snapshots` are written to disk separately using standard Go file I/O, users are discouraged from using this strategy in production settings. Refer to Project Notes.

## Testing

To verify the core mechanics (replication, election, and crash recovery) without manually executing HTTP requests, run the integrated lifecycle test:

```bash
go test -v .
```

This test spins up an in-process cluster, proposes data, simulates a node crash, and verifies that the node catches up on missed data after it recovers.

## Project Design

The design cleanly separates the application logic from the consensus logic:

1. **`kvstore` (Application API):** Represents the user-facing service. It receives HTTP requests, packages them into operations, and proposes them to the Raft cluster.
2. **`kvfsm` (State Machine):** The finite state machine strictly driven by the Raft consensus module. It only applies changes once the cluster has committed them.
3. **Strict Event Loop Ordering:** The implementation inside `raft.go` demonstrates the critical execution pipeline required by `etcd/raft`: Disk I/O (Persistence) -> In-Memory State Updates -> Network Broadcasts -> FSM Application.
4. **Periodic Snapshotting:** To prevent the Raft log from growing indefinitely, the node automatically creates a snapshot every 10,000 entries. This value is configurable in `raft.go` and provides a practical demonstration of log compaction.

## Project Notes

When experimenting with `raftsimple`, keep the following limitations in mind:

* **The Fixed ID Problem (Crash vs. Removal):** In the Raft protocol, if a node is formally removed via a `ConfChangeRemoveNode` proposal (not currently implemented in this example's API), its removal is recorded as a committed entry. Restarting that node with the same ID would cause it to see its own removal and immediately shut down. In `raftsimple`, the `DELETE` API simulates a **crash** (stopping the node's goroutines without removing it from the cluster configuration), which allows you to safely revive nodes with the same ID using `POST`.
* **Synchronous I/O:** To keep the code linear and easy to read, WAL writes in `raftsimple` are synchronous and unbuffered. They will block the Raft event loop until the file system completes the write. In a production environment facing high IOPS, this disk write would be decoupled into an asynchronous background goroutine.
