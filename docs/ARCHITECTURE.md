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

**As of Phase 1, none of the control plane exists.** There is one process. The job graph is
built in `main()`, handed to `LocalJobExecutor`, and run as one thread per subtask with bounded
queues between them. Kafka is real; everything else in the diagram arrives later.

```
              +------------------------------------------+
              |            One JVM (lms-job)             |
+---------+   |  +--------+    +-----------+   +-------+ |
|  Kafka  |-->|  | clicks | -> | drop-bots |-->|console| |
| 4 parts |   |  |  x4    |    |    x4     |   |  x2   | |
+---------+   |  +--------+    +-----------+   +-------+ |
              |        FORWARD          REBALANCE        |
              +------------------------------------------+
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
| `LocalOutput` | engine-runtime | Routes records by exchange strategy; broadcasts control elements down the channels it actually feeds | 1 |
| `TaskInstances` | engine-runtime | Gives each subtask a private copy of its operator, by serialization | 1 |
| `LocalJobExecutor` | engine-runtime | Single-JVM execution: a thread and a bounded queue per subtask | 1 |
| `KafkaSource` | engine-connectors | Reads JSON from Kafka. Phase 1 lets Kafka own offsets; Phase 4 takes them back | 1 |
| `ConsoleSink` | engine-connectors | Prints. An `Operator<T, Void>` — sinks are not a separate concept | 1 |
| `JobMaster` / `Scheduler` | engine-master | Compiles the physical graph, assigns tasks, owns the job state machine | 2 |
| `ExecutionGraph` compiler | engine-master | Expands operators to subtasks, builds chain groups, assigns round-robin | 2 |
| `EtcdMetadataStore` | engine-master | Durable job graph, assignments, checkpoint pointers | 2 |
| `RecordTransport` | engine-runtime | Serialization, gRPC streaming, credit-based flow control | 2 |
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
