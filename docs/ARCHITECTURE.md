# Architecture

Kept current as phases land. Components that do not exist yet are marked with the phase that
adds them, so this file also serves as a map of what is coming.

---

## Process topology

The target shape, from §4.1 of the companion PDF:

```
+-------------+     submit job      +----------------------------------+
|  JobClient  | ------------------> |            JobMaster             |
+-------------+ <-- status stream --|  Scheduler                       |
                                    |  CheckpointCoordinator           |
+-------------+   job graph         |  TaskTracker (heartbeats)        |
|    etcd     | <-- assignments --> |  StatusApi (REST)                |
| (metadata)  |     ckpt pointers   +----------------------------------+
+-------------+                        |     |                  ^
                     deploy task /     |     |                  | heartbeat
                     trigger ckpt      v     v                  | + ckpt ack
                                +----------+----------+----------+
                                | Worker 1 | Worker 2 | Worker 3 |
                                |  tasks   |  tasks   |  tasks   |
                                |  state   |  state   |  state   |
                                +----------+----------+----------+
                                     ^          |           ^
                                     +----------+-----------+
                                       record streams
                                       (gRPC, credit-based)
                                            |
                        +-------------------+-------------------+
                        |                                       |
                  +-----------+                        +----------------+
                  |   Kafka   | source                 |   MinIO / S3   |
                  +-----------+ (resettable offsets)   | ckpts + Iceberg|
                                                       +----------------+
```

**As of Phase 2 the control plane is real.** A master process serves `MasterService` on :7000;
three worker processes serve `WorkerService` and `DataTransportService` on separate ports and
register in etcd under a TTL lease. Records cross process boundaries over gRPC with credit-based
flow control. What is still missing from the diagram is everything downstream of a checkpoint:
MinIO, the Iceberg warehouse, and the coordinator that would write to them.

`LocalJobExecutor` remains, and is still the right tool for tests and demos -- a determinism test
comparing two runs of one fixture does not get more convincing by involving three processes.

The LMS job as Phase 2 actually schedules it. `clicks` and `drop-bots` fuse into one chain, so
the job is two chain groups and six vertical slices:

```
                          +------------------ etcd ------------------+
                          |  /jobs/{id}/graph /state /assignments    |
                          |  /workers/{id}   (TTL lease)             |
                          +------------------------------------------+
                               ^                        ^
                        submit |                        | register + watch
                               |                        |
   JobClient --SubmitJob--> master :7000 --DeployTask--> workers
                                  <--heartbeat (1s)----

   worker-1            worker-2                worker-3
   +-------------+     +-------------------+   +-------------------+
   | clicks:0    |     | clicks:1          |   | clicks:2          |
   | clicks:3    |     | console:0         |   | console:1         |
   +-------------+     +-------------------+   +-------------------+
          |                    ^                        ^
          +--- gRPC records, credit-based ---------------+

   Kafka (4 partitions) feeds every clicks subtask.
   clicks -> drop-bots is FORWARD and chained: one thread, a method call, no serialization.
   drop-bots -> console is REBALANCE: round-robin, across the network.
```

---

## Component table

| Component | Lives in | Responsibility | Phase |
|---|---|---|---|
| `StreamElement` / `StreamRecord` / `Watermark` / `CheckpointBarrier` | engine-api | The record envelope. Control elements travel in band with data, in order, on the same channel | 1 |
| `Operator` / `KeyedOperator` | engine-api | User logic. Single-threaded by contract; `Serializable` because it is shipped to a worker | 1 |
| `JobGraph` + `DataStream` / `KeyedStream` | engine-api | The logical graph and the builder that produces it. Validated on construction, not in the builder | 1 |
| `KeyGroupAssigner` | engine-api | `key → key group → subtask`. The indirection that lets parallelism change without rehashing state | 1 |
| `StateBackend` / `ValueState` / `ListState` | engine-api | Keyed state, narrow enough that heap and RocksDB are interchangeable | 1 (interfaces) |
| `OperatorTask` | engine-runtime | The run loop. Switches over all three element kinds; watermark and barrier branches stubbed | 1 |
| `SourceTask` | engine-runtime | Polls a source. Pull-based, which is what makes backpressure reach the broker | 1 |
| `ResultPartitionWriter` | engine-runtime | Routes records by exchange strategy; broadcasts control elements down the channels it actually feeds | 1 |
| `InputGate` | engine-runtime | One bounded queue per input channel. Reports which channel an element came from, and can block one without blocking the task | 2 |
| `OperatorChain` | engine-runtime | Fuses adjacent operators into one thread, exchanging records by method call | 2 |
| `UserCodeClassLoader` | engine-runtime | Loads the job classes the engine was never compiled against | 2 |
| `TaskInstances` | engine-runtime | Gives each subtask a private copy of its operator, by serialization | 1 |
| `LocalJobExecutor` | engine-runtime | Single-JVM execution: a thread and a bounded queue per subtask | 1 |
| `KafkaSource` | engine-connectors | Reads JSON from Kafka. Phase 1 lets Kafka own offsets; Phase 4 takes them back | 1 |
| `ConsoleSink` | engine-connectors | Prints. An `Operator<T, Void>` — sinks are not a separate concept | 1 |
| `JobMaster` | engine-master | Owns the job state machine; persists every transition before acting on it | 2 |
| `ExecutionGraph` compiler | engine-master | Expands operators to subtasks, builds chain groups, assigns round-robin | 2 |
| `EtcdMetadataStore` | engine-metadata | Durable job graph, assignments, checkpoint pointers; worker registry under a TTL lease | 2 |
| `ChainBuilder` | engine-master | The four conditions for fusing two operators | 2 |
| `GrpcTaskDeployer` | engine-master | Sends tasks to workers, sinks first, so nothing sends to a task that does not exist | 2 |
| `JobClient` | engine-master | Serializes a graph and submits it to the master | 2 |
| `DataTransportService` / `Client` | engine-runtime | gRPC record streams, batching, credit-based flow control | 2 |
| `StreamElementSerializer` | engine-runtime | The wire format: a tag byte plus payload | 2 |
| `TaskManager` | engine-worker | Turns a `TaskDeployment` into running threads | 2 |
| `HeartbeatClient` | engine-worker | Beats to the master and receives commands on the same stream | 2 |
| `TaskTracker` | engine-master | Heartbeats; a worker is dead after three missed beats | 2 |
| `WatermarkTracker` | engine-runtime | Per-channel watermarks, minimum across non-idle channels | 3 |
| `BoundedOutOfOrdernessGenerator` | engine-runtime | Watermark generation with idleness detection | 3 |
| `TimerService` | engine-runtime | Event-time timers per key, fired in timestamp order | 3 |
| `InMemoryStateBackend` | engine-runtime | Heap-backed keyed state | 3 |
| `CheckpointCoordinator` | engine-master | Triggers checkpoints, collects acks, notifies sinks | 4 |
| `BarrierAligner` | engine-runtime | Chandy-Lamport alignment across input channels | 4 |
| `RocksDbStateBackend` | engine-runtime | Embedded state, snapshotted to MinIO | 4 |
| `IntervalJoinOperator` | engine-runtime | Two-sided buffering with watermark-driven eviction | 5 |
| `IcebergSink` | engine-connectors | Two-phase commit against an Iceberg table | 6 |
| `StatusApi` | engine-master | REST endpoints and Prometheus scrape | 7 |

---

## Decisions worth recording

**Control elements travel in band.** Watermarks and barriers are `StreamElement`s on the same
ordered channel as records. This is the structural decision the rest of the engine rests on: a
task can reason about "everything before this marker" from arrival order alone, with no side
channel to correlate and no clock to trust.

**A task is one thread.** Operator code never needs to be thread-safe. Parallelism comes from
more subtasks, not more threads inside one.

**Queues are bounded.** Backpressure is not a feature that was added; it is what a bounded queue
does when you let it block. Phase 2 has to rebuild this deliberately as credit-based flow
control, because a network gives nothing away for free.

**Validation lives on `JobGraph`, not its builder.** Phase 2 reads graphs back from etcd, and
that path has no builder to go through. A side effect is that a cycle cannot be constructed
through the fluent API at all, so the cycle test assembles node records directly.

**Sinks are `Operator<IN, Void>`.** Not a separate interface. The run loop stays uniform, and
`IcebergSink` in Phase 6 is written exactly as the companion PDF §12.1 shows it.

**`keyBy` creates no node.** Keying is a property of the next edge, not a step that does work.
Materialising it would mean a thread that only rehashes. The user's name for it is carried on
the edge so logs and metrics can use it.

**A slot is one vertical pipeline slice.** Scheduling assigns subtask *i* of every operator in a
chain group together, not whole chain groups. Assigning whole groups would cap a job's spread at
its number of chain groups however parallel its operators were -- the LMS job has two, so on
three workers one would always sit idle. See CLAUDE.md section 4.2.

**Deployment runs backwards.** Sinks are deployed first and sources last, because a task starts
running the instant it is deployed. Deploying forwards means a source producing records before
the filter behind it exists, and those records are dropped silently and only sometimes.

**Credit is per channel, not per connection.** TCP applies backpressure, but one connection
carries several logical channels, so a single slow subtask would stall its siblings. Credit moves
the decision to the only place that can make it correctly. It also keeps data out of socket
buffers, which from Phase 4 bounds how long barrier alignment can take.

**Long-lived streams are opened under `Context.ROOT`.** A gRPC client call started inside a
server handler inherits that handler's Context, which is cancelled when the handler returns.

**Exchange strategy belongs to a node's input, not its output.** One operator feeding two
downstreams may be routed two different ways. Attaching the strategy to the consumer keeps that
expressible.

**A forward edge between unequal parallelism becomes a rebalance.** Four filters into two sinks
has no subtask-to-subtask correspondence. Rejecting it would make the natural way of writing a
narrower sink an error; downgrading is safe because a forward edge is never keyed.

---

## Deviations from the companion PDF

| PDF | Here | Why |
|---|---|---|
| `MAX_PARALLELISM` | `NUM_KEY_GROUPS`, with `maxParallelism` as the job knob | CLAUDE.md §4.1: the PDF name conflates two things |
| `ValueState.value()` returns `T`, may be null | Returns `Optional<T>` | CLAUDE.md §2 forbids null in public APIs |
| `lms-job` depends on `engine-api` only | Also on `engine-connectors` | Otherwise the job cannot name `KafkaSource`. See CLAUDE.md §3 amendment |
| Linear fluent chain (§5.4) | Typed stream handles | A single chain cannot name the two inputs of Phase 5's interval join |
| Window DSL (§5.4) | Operator manages its own session state (§9.3) | The PDF contradicts itself; §9.3 is what CLAUDE.md Phase 3 mandates, and it shows the mechanism |
| Job state machine diagram (§8.2) | Phase 2 fails the job; Phase 4 adds `RESTARTING` | The PDF's diagram has its phase annotations transposed |
| Scheduling: "round-robin subtasks" vs "chained groups land together" (§8.1) | A slot is one vertical slice | The two halves of the PDF's sentence contradict each other; its prose says vertical slices, its snippet says whole groups. See CLAUDE.md section 4.2 |
| Six modules | Eight | `engine-rpc` and `engine-metadata` hold contracts both ends of the control plane need. See CLAUDE.md section 3 |
| `Optional` fields on graph nodes | Nullable components, `Optional` accessors | `java.util.Optional` is deliberately not serializable, and the graph is written to etcd |
