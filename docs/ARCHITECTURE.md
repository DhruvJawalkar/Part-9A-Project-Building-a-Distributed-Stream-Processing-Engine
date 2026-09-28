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

**As of Phase 4 the control, data and checkpoint paths are real.** A master process serves
`MasterService` on :7000;
three worker processes serve `WorkerService` and `DataTransportService` on separate ports and
register in etcd under a TTL lease. Records cross process boundaries over gRPC with credit-based
flow control. Source tasks now generate event-time watermarks and in-band idle/active status;
operator tasks advance their local clock from the minimum across active input channels. The master
injects source barriers and receives task checkpoint acknowledgements. Workers archive full task
snapshots in MinIO, while etcd stores the pointer and per-task handles only after the whole
checkpoint completes. The Iceberg warehouse and transactional sink remain Phase 6 work; MinIO is
currently used for checkpoint archives.

`LocalJobExecutor` remains, and is still the right tool for tests and demos -- a determinism test
comparing two runs of one fixture does not get more convincing by involving three processes.

The LMS job as Phase 3 actually schedules it. `clicks` and `drop-bots` fuse into one ordinary
chain; the keyed `sessions` operator and narrower `console` sink are separate chain groups. The
job therefore has ten task instances: four source/filter slices, four session slices, and two
sink slices. Their exact worker assignment follows the registered-worker order, but every group
is distributed round-robin:

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
   | chain slices|     | chain slices      |   | chain slices      |
   | source/filter|    | sessions          |   | console           |
   +-------------+     +-------------------+   +-------------------+
          |                    ^                        ^
          +--- gRPC records, credit-based ---------------+

   Kafka (4 partitions) feeds every clicks subtask.
   clicks -> drop-bots is FORWARD and chained: one thread, a method call, no serialization.
   drop-bots -> sessions is HASH by member id: key groups route one member to one subtask.
   sessions -> console is REBALANCE: round-robin, across the network.
```

---

## Event-time and keyed-state flow

The event-time path is deliberately a data-plane extension, not a separate scheduler. Records,
watermark claims, idleness transitions, and checkpoint barriers all use the same
ordered channel. That gives every task one answer to “what happened before this marker?”

```text
source record (event timestamp)
        |
        v
BoundedOutOfOrdernessGenerator
  max event time - allowed disorder
  silent source -> Watermark.IDLE
        |
        v                         keyBy(memberId)
SourceTask -- StreamRecord --> ResultPartitionWriter -- HASH/key group --> OperatorTask
        |                               |                                  |
        +-- Watermark / idle status ----+-------------------------------> WatermarkTracker
                                                                         min(active channels)
                                                                                |
                                                                                v
                                                   RocksDbStateBackend <- current key -> TimerService
                                                                                |
                                                                                v
                                                                    SessionAggregator callback
```

`Watermark.IDLE` is an explicit in-band transition because an absent message on a distributed
channel is ambiguous: it could mean an idle source, congestion, or a failed sender. A resumed
source sends `Watermark.ACTIVE` before its next record. `WatermarkTracker` excludes idle channels
from its minimum and only forwards a watermark if it advances; the timer service then fires all
due timers in timestamp order, restoring the key before each callback. A bounded source emits
`Watermark.MAX` only on EOF. An operator forwards MAX only after all of its input channels have
ended, preventing an idle channel from converting one upstream EOF into a false whole-job EOF.

The HASH exchange and the keyed runtime must agree on the key selector. The deployer serializes
the selector on the downstream edge; `ResultPartitionWriter` applies it for routing and
`OperatorTask` applies it before user code accesses state. This duplicate carriage is deliberate:
it makes a bad selector deployment fail instead of silently splitting a key’s records and state.

---

## Phase 4 checkpoint and recovery flow

Checkpoint barriers are `StreamElement`s, so each network channel preserves their order relative to
records. The coordinator never snapshots downstream tasks directly; it triggers sources and lets
the barrier mark the cut through the DAG:

```text
JobMaster / CheckpointCoordinator
        | TriggerCheckpoint (source tasks only)
        v
KafkaSource -> source snapshot + barrier -> ordered record channels
                                              |
                         +--------------------+-------------------+
                         v                                        v
                multi-input task                         single-input task
                block arrived channel                    snapshot directly
                read remaining channels                  forward barrier
                snapshot after all barriers              acknowledge
                forward -> acknowledge                   continue
                         |                                        |
                         +--------------- task acks -------------+
                                              |
                                  all tasks acknowledged?
                                              |
                               write completed pointer to etcd
                                              |
                         notify completion / future sink commit
```

For multi-input tasks, `BarrierAligner` blocks the channel that has delivered its barrier and keeps
consuming channels that have not. After all barriers for the same checkpoint arrive, the runtime
takes its snapshot, forwards the barrier, acknowledges to the master, and releases the blocked
channels. This explicit ordering prevents buffered post-barrier records from passing the barrier
downstream or entering the snapshot. If the coordinator times out, `AbortCheckpoint` releases any
channels held by the abandoned alignment.

Each operator task stores keyed state in RocksDB. Its archive contains the RocksDB checkpoint and
an envelope for pending timers and watermark progress. Each source archive contains connector
position and watermark-generator progress. Kafka's manual partition assignment snapshots the next
offset per partition without committing it to Kafka. Restore seeks to that position before source
polling starts.

Workers publish complete task directories through the `CheckpointStorage` interface. Production
cluster configuration selects `MinioCheckpointStorage`; it uploads a ZIP to the configured bucket
and returns a `minio://` handle. State handles nested inside the archive are relative paths. During
restore, the assigned worker fetches and extracts the archive to its local checkpoint root, making
the handle independent of the original worker's disk. Tests can use the filesystem implementation.

The coordinator holds acknowledgements in memory until every physical chain-group task reports a
handle. Then it persists the complete set to etcd before notifying workers of completion. An
incomplete checkpoint times out, is never selected for recovery, and sends an abort command. Worker
heartbeats include per-task checkpoint id, duration, state bytes, and alignment milliseconds.

Recovery always rewinds the whole DAG. The master cancels and joins each running task, reads the
latest completed pointer, then sends every task its matching state handle during redeployment. It
quarantines a dead worker until a fresh registration, preventing recovery from assigning tasks to
a stale etcd lease. The default restart policy waits one second and allows three attempts before
marking the job failed.

Heartbeat clients reconnect after a master outage. A surviving worker's first beat seeds the
replacement master's failure detector, and task-status samples repopulate its latest-status
registry. A bounded job transitions to `FINISHED` only when every task in the recovered physical
plan reports completion.

### Verified recovery run

`DistributedCheckpointRecoveryIT` is the automated process-level acceptance test, run with
`./gradlew :lms-job:integrationTest`. Testcontainers provides etcd and MinIO, and the test starts
a real master plus three worker child JVMs. The five-task bounded job completes a checkpoint with
zero bytes emitted by the sink. It force-kills the session owner chosen by `KeyGroupAssigner`,
verifies task reassignment excludes the dead worker, waits for `FINISHED`, and compares the output
byte for byte with a clean baseline.

The three-worker run had 10 tasks. Five checkpoints completed with 10 task handles each in etcd
and MinIO. Killing worker-2 caused detection after three missed heartbeats (about 3.47 seconds); the
master recorded `clicks:1`, `sessions:0`, and `sessions:3` as lost, cancelled the whole job, and
restarted from checkpoint 5 on worker-1 and worker-3. The replacement assignments excluded
worker-2, and checkpoint 6 onward completed with all 10 handles. The `minio://` handles contained
source-position properties and operator snapshots.

The separate live Kafka run verifies recovery for the full 10-task LMS plan and MinIO checkpoint
wiring. The fixed-replay `SessionPipelineRecoveryAcceptanceTest` and MinIO cross-filesystem test
provide additional focused coverage, including restore after deleting the producer's local
checkpoint directory.

---

## Component table

| Component | Lives in | Responsibility | Phase |
|---|---|---|---|
| `StreamElement` / `StreamRecord` / `Watermark` / `CheckpointBarrier` | engine-api | The record envelope. A watermark carries active/idle status as well as a timestamp; control elements travel in band with data | 1–3 |
| `Operator` / `KeyedOperator` | engine-api | User logic. Single-threaded by contract; `Serializable` because it is shipped to a worker | 1 |
| `JobGraph` + `DataStream` / `KeyedStream` | engine-api | The logical graph and the builder that produces it. Validated on construction, not in the builder | 1 |
| `KeyGroupAssigner` | engine-api | `key → key group → subtask`. The indirection that lets parallelism change without rehashing state | 1 |
| `StateBackend` / `ValueState` / `ListState` | engine-api | Keyed state, narrow enough that heap and RocksDB are interchangeable | 1 (interfaces) |
| `OperatorTask` | engine-runtime | Restores keyed context, advances watermarks/timers, aligns multi-input barriers, snapshots and forwards before ack | 1–4 |
| `SourceTask` | engine-runtime | Polls a source, assigns event time, checkpoints connector position and emits watermarks/barriers | 1–4 |
| `ResultPartitionWriter` | engine-runtime | Routes FORWARD/REBALANCE/BROADCAST/HASH records and broadcasts control elements only down channels it actually feeds | 1–3 |
| `InputGate` | engine-runtime | One bounded queue per input channel. Reports which channel an element came from, and can block one without blocking the task | 2 |
| `OperatorChain` | engine-runtime | Fuses adjacent operators into one thread, exchanging records by method call | 2 |
| `UserCodeClassLoader` | engine-runtime | Loads the job classes the engine was never compiled against | 2 |
| `TaskInstances` | engine-runtime | Gives each subtask a private copy of its operator, by serialization | 1 |
| `LocalJobExecutor` | engine-runtime | Single-JVM execution: a thread and a bounded queue per subtask | 1 |
| `KafkaSource` | engine-connectors | Manual partition ownership; snapshots next offsets and seeks to checkpointed positions on restore | 1, 4 |
| `FileReplaySource` | engine-connectors | Bounded, deterministic JSON Lines replay partitioned by stable line hash | 3 |
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
| `WatermarkTracker` | engine-runtime | Per-channel watermarks and active/idle status; emits only a forward-moving minimum | 3 |
| `BoundedOutOfOrdernessGenerator` | engine-runtime | Source watermark generation from max event time minus disorder, with idleness detection | 3 |
| `TimerService` | engine-runtime | Deduplicated key/timestamp timers, fired in timestamp order with key context restored | 3 |
| `InMemoryStateBackend` | engine-runtime | Heap-backed keyed value/list state for local execution and tests | 3 |
| `CheckpointCoordinator` | engine-master | Periodic trigger, all-task ack collection, etcd completion pointer, timeout/abort, completion notification | 4 |
| `BarrierAligner` | engine-runtime | Blocks arrived channels; snapshot → forward → ack → release, with abort support | 4 |
| `RocksDbStateBackend` | engine-runtime | Keyed value/list state and RocksDB snapshots for task checkpoints | 4 |
| `CheckpointStorage` | engine-worker | Archives/materializes full task checkpoints; filesystem and MinIO implementations | 4 |
| `RestartStrategy` | engine-master | Fixed-delay job-wide restart attempts | 4 |
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

**A keyed operator is a chain boundary.** A keyed task owns one current-key context, one state
backend and one timer service. Chaining an ordinary neighbour into that task would let it observe
the keyed operator's state namespace. The engine keeps the boundary explicit until a future
runtime has per-operator state namespaces inside a chain.

**Idleness is explicit, not inferred.** No message is not a reliable signal on a distributed
channel. `Watermark.IDLE` and `Watermark.ACTIVE` make the liveness decision visible, ordered with
the data it affects, and testable.

**EOF requires every channel.** `Watermark.MAX` means a channel is complete, not that a task is
complete. The task advances to MAX only after every input has ended, even if some inputs were
previously idle.

**A checkpoint is complete only as a whole.** The coordinator does not publish a recovery pointer
until every physical task has acknowledged. Worker cancellation also joins the prior task thread
before a restored task with the same identity may be deployed; otherwise both histories could
emit after one checkpoint.

**Checkpoint handles must outlive their worker.** Local RocksDB files are staging state only.
Workers archive the whole task directory to shared object storage before acknowledging, and nested
handles use paths relative to that archive. This lets a recovered task land on a different worker.

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
