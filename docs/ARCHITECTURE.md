# Architecture

Kept current as phases land. Components that do not exist yet are marked with the phase that
adds them, so this file also serves as a map of what remains.

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
                  +-----------+ (checkpoint offsets)   | checkpoints    |
                                                       +----------------+
```

**All seven phases are implemented.** A master process serves
`MasterService` on :7000;
three worker processes serve `WorkerService` and `DataTransportService` on separate ports and
register in etcd under a TTL lease. Records cross process boundaries over gRPC with credit-based
flow control. Source tasks now generate event-time watermarks and in-band idle/active status;
operator tasks advance their local clock from the minimum across active input channels. The master
injects source barriers and receives task checkpoint acknowledgements. Workers archive full task
snapshots in MinIO, while etcd stores the pointer and per-task handles only after the whole
checkpoint completes. The Iceberg REST catalog stores table metadata in the MinIO warehouse, and
the LMS sinks publish completed checkpoint intervals to Iceberg only after that durable pointer
write. `StatusApi`, worker metrics servers, Prometheus, and Grafana expose the live task state
used by the runtime instead of maintaining a second observability model.

`LocalJobExecutor` remains, and is still the right tool for tests and demos -- a determinism test
comparing two runs of one fixture does not get more convincing by involving three processes.

The LMS job now has two source paths and two outputs. Clean clicks fan out after bot filtering:
one path is keyed by member id for sessions, while result clicks are tagged and unioned with
tagged borrows before conversion matching. The union forces a multi-input task boundary and uses
`REBALANCE`; the conversion operator is keyed by member and catalog item. Tasks are assigned by
vertical slice across the registered workers:

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

   Kafka clicks (4 partitions) -> drop-bots --+-- HASH(memberId) -> sessions -> browse_sessions sink (1)
                                               |
                                               +-> result-clicks -> tag-left --+
                                                                                +-> union
   Kafka borrows (4 partitions) -------------------------------> tag-right ----+  REBALANCE
                                                                                   |
                                                                                   +-> HASH(ConversionKey)
                                                                                       -> interval join
                                                                                       -> click_conversions sink (1)

   Worker tasks exchange records over gRPC with credit-based flow control. Each producer edge
   retains its own exchange strategy; every union input subtask has a separate input-channel id.
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
                         notify completion / Iceberg sink commit
```

For multi-input tasks, `BarrierAligner` blocks the channel that has delivered its barrier and keeps
consuming channels that have not. After all barriers for the same checkpoint arrive, the runtime
takes its snapshot, forwards the barrier, acknowledges to the master, and releases the blocked
channels. This explicit ordering prevents buffered post-barrier records from passing the barrier
downstream or entering the snapshot. If the coordinator times out, `AbortCheckpoint` releases any
channels held by the abandoned alignment.

Each operator task stores keyed state in RocksDB. Its archive contains the RocksDB checkpoint and
an envelope for pending timers and watermark progress. An operator implementing
`CheckpointListener` runs `preCommit` after alignment and before the snapshot; the serializable
return value is persisted in that same task envelope and restored after `open()`. Each source
archive contains connector position and watermark-generator progress. Kafka's manual partition
assignment snapshots the next offset per partition without committing it to Kafka. Restore seeks
to that position before source polling starts.

Workers publish complete task directories through the `CheckpointStorage` interface. Production
cluster configuration selects `MinioCheckpointStorage`; it uploads a ZIP to the configured bucket
and returns a `minio://` handle. State handles nested inside the archive are relative paths. During
restore, the assigned worker fetches and extracts the archive to its local checkpoint root, making
the handle independent of the original worker's disk. Tests can use the filesystem implementation.

The coordinator holds acknowledgements in memory until every physical chain-group task reports a
handle. Then it persists the complete set and recovery pointer to etcd before notifying workers of
completion. Completion and abort callbacks are queued onto the operator task's own run loop, not
called from gRPC handler threads. An incomplete checkpoint times out, is never selected for
recovery, and sends an abort command. Worker heartbeats include per-task checkpoint id, duration,
state bytes, and alignment milliseconds.

Recovery always rewinds the whole DAG after worker loss or master restart. The master cancels and
joins each running task, reads the latest completed pointer, then sends every task its matching
state handle during redeployment. The master-restart rewind fences any checkpoint id that existed
only in the previous coordinator's memory. It
replays the durable checkpoint's sink completion callback after restore; transactional sinks make
this safe when a prior master died after committing but before observing the callback. It quarantines
a dead worker until a fresh registration, preventing recovery from assigning tasks to a stale etcd
lease. The default restart policy waits one second and allows three attempts before marking the job
failed.

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

## Phase 6 transactional Iceberg output

The Iceberg path extends the task checkpoint envelope without adding a second sink protocol:

```text
aligned barrier
      |
      v
IcebergSink.preCommit(checkpoint)
      | closes writer for this interval
      v
task envelope: keyed state + timers + watermarks + pending file path/size/rows
      |
      v
all task acks -> etcd durable completed pointer -> task-thread completion callback
                                                               |
                                                               v
                                      refresh current table; append missing paths once
```

`IcebergSink` owns one Parquet writer per checkpoint interval. `preCommit` closes that writer,
stores its path, size and row count in serializable pending-file state, and opens the next writer.
The object is deliberately not visible to an Iceberg scan at this point. After the master has
written the completed-checkpoint pointer and handles to etcd, the worker queues
`notifyCheckpointComplete` on the sink's operator thread. The sink refreshes the table and scans
reachable data-file paths before appending missing pending files in one metadata commit. This
current-table path check makes callback replay idempotent after a crash between the table commit
and the completion acknowledgement. Abort drops pending metadata; a file already in object
storage remains an orphan outside every reachable snapshot and contributes no rows.

The LMS graph uses singleton sink parallelism (one subtask per table), so each checkpoint interval
is one append to the exact tables `lms.analytics.browse_sessions` and
`lms.analytics.click_conversions`. `LmsIcebergOutputs` reads configuration in the submitter and
passes serializable REST catalog settings; workers create the catalog and S3 file IO locally from
those captured values. `docker compose up -d` starts Kafka, etcd, MinIO
and `apache/iceberg-rest-fixture`. MinIO supplies `stream-checkpoints` and `warehouse` buckets;
the REST catalog reaches it as `http://minio:9000`, while host-launched workers use
`http://localhost:9000` and the catalog at `http://localhost:8181`.
`demos/iceberg/init-schema.sh` idempotently creates namespace `lms.analytics` and both
unpartitioned tables; `demos/run-cluster.sh` runs that bootstrap automatically as well.

### Acceptance evidence

`IcebergSinkAcceptanceTest` uses an in-memory Iceberg catalog and local file IO. It checks that rows
remain invisible before completion, completion replay does not create a duplicate snapshot or row,
and simulated lost-sink replay leaves the old closed file as an orphan while replacement output
exposes each logical row once. The fixed six-event cadence evidence is:

| Checkpoint cadence | First visible row | Data files | Snapshots | Rows |
|---|---:|---:|---:|---:|
| Short | 100 ms | 3 | 3 | 6 |
| Long | 500 ms | 1 | 1 | 6 |

The shorter cadence improves freshness but creates more small files. Partitioned Iceberg tables are
not supported in Phase 6; the sink rejects them explicitly at open time. The REST/MinIO Compose
surface is executable demo infrastructure. The opt-in `IcebergRestMinioSmokeTest` verifies its
REST catalog, `S3FileIO`, MinIO write and completion path, but is not a process-kill test.
The transaction guarantee covers completed checkpoint intervals. Bounded sources do not yet wait
for a coordinator-owned terminal checkpoint, so their final post-barrier interval is a documented
limitation; the LMS production sources are unbounded Kafka inputs.

---

## Phase 5 multi-input interval join

The job API keeps two-input handling visible without making the runtime's task loop a special
case for two physical inputs. `Either<L,R>` tags each element in one stream; `IntervalJoinOperator`
is a keyed operator over that tagged stream:

```text
click source -> bot filter --+-- HASH(memberId) -> session branch
                             +-- result click -> Either.left --+
borrow source ---------------------------> Either.right ------+-- union(REBALANCE)
                                                                -> HASH(ConversionKey)
                                                                -> IntervalJoinOperator
```

`DataStream.union(id, other)` creates a timestamp-preserving pass-through transform with two
upstream edges. `REBALANCE` gives each downstream subtask a channel for each upstream subtask;
`FORWARD` cannot express fan-in when branches have different parallelisms or no one-to-one
subtask correspondence. The compiler leaves the union at a multi-input task boundary, where
watermark and barrier alignment can still identify each input independently.

`FanOutOutput` handles the other side of the graph shape: when clean clicks feed both the session
and conversion branches, it delegates each element and control marker to a separate
edge-specific output. Each output keeps its own exchange and key selector. Input channels retain
distinct upstream operator/subtask identity, rather than collapsing arrivals from both branches
into one anonymous queue.

The public API in `engine-api` consists of sealed `Either<L,R>`, `JoinFunction<L,R,O>`, and
`IntervalJoinOperator<K,L,R,O>`. A pair matches when
`lowerBound <= rightTimestamp - leftTimestamp <= upperBound`. The LMS config uses lower bound 0
and upper bound 30 minutes, inclusive. Each new left or right record is checked against the
opposite keyed `ListState`, then retained on its own side; the pair emits once when the second
arrival is processed, regardless of which source arrived first.

The join keeps `interval-join-left` and `interval-join-right` lists per `ConversionKey`, and exposes
`left-buffer-size` and `right-buffer-size` gauges. Every retained record registers a timer. For
this 0..30-minute forward interval, a left record at `t` is evicted only when
`t + upperBound < watermark`; a right record at `t` is evicted only when
`t < watermark - upperBound`. The strict predicates preserve the inclusive boundary matches
while the watermark equals their last possible match time. A timer callback restores its key and
evaluates the current watermark before removing expired entries.

The deterministic borrow fixture is `demos/fixtures/borrows.jsonl` and is published by
`demos/seed-borrows.sh`; clicks and borrows can be seeded in either order. If one branch lags,
the combined watermark follows the slower active channel. The fast branch's unmatched records
remain in its buffer until that watermark passes their cleanup horizon, making the slower-stream
effect visible in the two gauges.

The session branch remains keyed by `memberId`; the conversion branch is keyed by
`ConversionKey(memberId, catalogItemId)`. Both branches now terminate in singleton,
checkpoint-transactional Iceberg sinks: `lms.analytics.browse_sessions` and
`lms.analytics.click_conversions`.

## Phase 7 observability and operational topology

The REST layer is deliberately a projection of master-owned state. It does not schedule work or
introduce a second job model:

```text
workers -- heartbeat(TaskStatus + partition lag) --> JobMaster
   |                                                   |
   +-- /metrics (:8081)                                +-- /jobs/... (:8080)
   |                                                   +-- /metrics (:8080)
   +-------------------------- Prometheus <------------+
                                      |
                                      v
                            provisioned Grafana dashboard
```

`TaskStatus` carries records in/out, watermark, checkpoint duration/alignment/state bytes, real
input-queue occupancy, and Kafka source partition lag. The `backpressured` value is derived from
the bounded `InputGate` rather than guessed from throughput. `KafkaSource` samples broker end
offsets on its own poll thread at most once per second; the reporting thread reads the cached
snapshot, preserving KafkaConsumer's single-threaded contract. Lag in records is exact at the
sample. Unread-record age is not derivable without fetching those records, so `maxLagMillis` is
explicitly `null` instead of a fabricated value.

`startedAt` is the current master's admission time and becomes `null` after master recovery; the
metadata schema does not persist a submission timestamp. Per-task `watermark` is also `null`
because it has not yet been added to heartbeats. Checkpoint counts and history are operational
state for the current master, while etcd intentionally persists only the latest complete recovery
point. These gaps are surfaced as null or reset values rather than inferred from unrelated clocks.

Prometheus retains `job`, `operator`, `subtask`, and (on workers) `worker` labels. Those labels are
not presentation detail: Demo 4 depends on seeing one session subtask saturate while its siblings
are idle. Grafana provisions exactly four panels: source lag; checkpoint duration and alignment;
records-in per subtask; and checkpoint state size per subtask.

The Compose stack is also the deployment specification. One multi-stage Dockerfile builds master,
worker, LMS, and submitter artifacts. Three worker containers advertise service-discovery names
to the master; etcd stores control-plane state; MinIO stores checkpoint archives and the Iceberg
warehouse. One-shot services create topics, buckets, Iceberg schemas, submit the graph, and publish
the fixed fixtures. Consequently `docker compose up -d --build` starts an inspectable system, not
just its dependencies.

### Skew mitigation topology

The normal session branch uses `HASH(memberId)`, which is exact but gives one hot member to one
subtask. The optional `-Dlms.sessions.salted=true` path introduces two visible stages:

```text
ClickEvent
  -> deterministic salt(payload) in [0, 16)
  -> HASH(memberId, salt)
  -> local incremental session fragment
  -> HASH(memberId)
  -> global fragment merge
  -> SessionRow
```

Local shards close ordinary session fragments. The global merge waits an extra session gap so
fragments from every salt have become event-time complete, but retains the normal business end in
the output row. State remains incremental: compact counts and distinct search terms cross the
second shuffle, never the original event list. Since `ClickEvent` has no immutable event id,
identical payloads choose the same stable salt; this is required for deterministic replay.

The unsalted and salted fixed-replay acceptance paths emit identical rows. The hot-key fixture
also models the actual key-group mapping: the unsalted owner exceeds its service capacity while
three siblings receive nothing, whereas salted local keys use all four subtasks below capacity.

### Late-data policy

The zero-argument `SessionAggregator` drops and counts an event behind the current watermark.
Supplying an allowed-lateness duration may revise the still-retained open session and increments a
separate accepted-late counter. Closed sessions are not kept for later rewriting, and the Iceberg
sink is append-only. Reopening historical output would require an upsert/equality-delete contract;
the engine reports this boundary rather than implying that an append is an overwrite.

---

## Component table

| Component | Lives in | Responsibility | Phase |
|---|---|---|---|
| `StreamElement` / `StreamRecord` / `Watermark` / `CheckpointBarrier` | engine-api | The record envelope. A watermark carries active/idle status as well as a timestamp; control elements travel in band with data | 1–4 |
| `CheckpointListener` | engine-api | Optional operator lifecycle: pre-commit state in the task envelope, restore it, and receive durable completion/abort callbacks | 6 |
| `Operator` / `KeyedOperator` | engine-api | User logic. Single-threaded by contract; `Serializable` because it is shipped to a worker | 1 |
| `JobGraph` + `DataStream` / `KeyedStream` | engine-api | The logical graph and the builder that produces it. Validated on construction, not in the builder | 1 |
| `DataStream.union` | engine-api | Timestamp-preserving fan-in with `REBALANCE` edges and a multi-input boundary | 5 |
| `Either` / `JoinFunction` | engine-api | Public left/right tag and pair-to-output function for a logical two-stream operator | 5 |
| `KeyGroupAssigner` | engine-api | `key → key group → subtask`. The indirection that lets parallelism change without rehashing state | 1 |
| `StateBackend` / `ValueState` / `ListState` | engine-api | Keyed state, narrow enough that heap and RocksDB are interchangeable | 1 (interfaces) |
| `OperatorTask` | engine-runtime | Restores keyed context, advances watermarks/timers, aligns multi-input barriers, snapshots and forwards before ack | 1–4 |
| `SourceTask` | engine-runtime | Polls a source, assigns event time, checkpoints connector position and emits watermarks/barriers | 1–4 |
| `ResultPartitionWriter` | engine-runtime | Routes FORWARD/REBALANCE/BROADCAST/HASH records and broadcasts control elements only down channels it actually feeds | 1–5 |
| `FanOutOutput` | engine-runtime | Dispatches records and control elements to separate edge-specific outputs | 5 |
| `InputGate` | engine-runtime | One bounded queue per upstream operator/subtask channel; preserves identity for watermarks and barriers | 2–5 |
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
| `IntervalJoinOperator` | engine-api | Keyed, timestamp-bounded matching with separate left/right `ListState` and cleanup gauges | 5 |
| `IcebergSink` | engine-connectors | One Parquet writer per checkpoint interval; pending file state and idempotent current-table append for unpartitioned Iceberg tables | 6 |
| `StatusApi` | engine-master | REST endpoints and Prometheus scrape | 7 |
| `WorkerMetricsServer` | engine-worker | Per-task Prometheus scrape with queue and checkpoint metrics | 7 |
| `SaltedSessionAggregator` | lms-job | Deterministic local-then-global session aggregation for hot keys | 7 |

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

**Sinks are `Operator<IN, Void>`.** Not a separate interface. The run loop stays uniform.
`IcebergSink` adds the optional `CheckpointListener` lifecycle: its interval file is prepared in
task state and becomes visible only after the durable checkpoint callback.

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
