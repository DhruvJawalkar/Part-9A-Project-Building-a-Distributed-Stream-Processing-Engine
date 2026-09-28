# Engine design

This living design document records what is implemented through **Phase 5** and labels later
work as **planned**. This is a teaching engine: its source shows the mechanism directly instead
of hiding it behind a production framework.

The companion rationale is in [Part9A_Project_Companion.pdf](Part9A_Project_Companion.pdf); the
authoritative build sequence is in [CLAUDE.md](../CLAUDE.md).

## Status

| Phase | Status | Outcome |
|---|---|---|
| 1 | Complete | Local task runtime, logical DAG, Kafka source and console sink |
| 2 | Complete | Master/worker deployment, etcd, gRPC transport and failure detection |
| 3 | Complete | Event time, watermarks/idleness, keyed state/timers and sessions |
| 4 | Complete | Aligned checkpoints, RocksDB state, MinIO archives and whole-job recovery |
| 5 | Complete | Tagged two-stream interval join and LMS conversion branch |
| 6 | **Planned** | Transactional Iceberg output |
| 7 | **Planned** | Status API, dashboard and reproducible demos |

## Core rules

- **Control travels in band.** `StreamRecord`, `Watermark`, and `CheckpointBarrier` use the same
  ordered channels. A task can reason about what happened before a marker without a side protocol.
- **One task, one thread.** Operators, their state, timer callbacks and collectors run on one
  task thread. Parallelism comes from subtasks, not locks within user code.
- **State is keyed ambient context.** Operators acquire state in `open()`. The runtime makes a
  key current before a record or timer callback, so one `ValueState` handle resolves per key.
- **The graph is logical before physical.** Job code describes transformations; the master makes
  subtasks, exchanges, chain groups and worker assignments.
- **Legibility wins.** A keyed task is a chain boundary until state namespaces can be per-operator
  inside a fused chain.
- **Recovery follows one distributed cut.** A task failure cancels and restores every task from
  the same completed checkpoint. Restarting only the failed task could pair replayed input with
  downstream tasks that had already processed beyond that point.

## Computation DAG

`JobGraph` is a serializable logical DAG. Nodes have stable, user-supplied identifiers; edges
describe how a consumer receives input. Validation rejects cycles, unreachable operators, and
non-positive parallelism.

```text
clicks -> drop-bots --+-- HASH by memberId -> sessions -> session console
                      |
                      +-- result-clicks -> tag-left --+
                                                     +-- REBALANCE union
borrows ---------------------------> tag-right ------+       |
                                                             +-- HASH by ConversionKey
                                                             +-- IntervalJoinOperator
                                                             +-- conversion console
```

`keyBy` does not create a node. It records a `KeySelector` and partition name on the next input
edge. This makes keying visible at the exchange which routes records and at the task which owns
their state.

### Logical-to-physical compilation

`ExecutionGraphCompiler` expands a logical operator into numbered subtasks. `KeyGroupAssigner`
performs the stable two-hop mapping:

```text
key -> fixed key group (0..127) -> subtask at the chosen parallelism
```

The fixed group count separates stable key placement from task placement. A future rescale can
move whole key groups; dynamic rescaling is not implemented.

`ChainBuilder` fuses neighbouring operators only when the consumer input is `FORWARD`, both sides
have equal parallelism, the producer has one downstream, and the consumer has one upstream. It
also refuses to chain either side if it is keyed. The scheduler assigns vertical slices—subtask
*i* of a chain group—round-robin across workers. Deployment is reverse topological: sinks first,
then sources, so a source never sends before its destination exists.

## Master-worker control plane

```text
JobClient -- SubmitJob --> JobMaster -- DeployTask --> Worker TaskManager
                              |                         |
                              +-- etcd graph/state ------+
                              ^                         |
                         heartbeat <--------------------+
```

- `JobMaster` owns the job state machine and persists transitions in etcd before acting.
- Workers register under TTL leases. `TaskTracker` watches registrations and treats three missed
  one-second heartbeats as worker loss.
- `GrpcTaskDeployer` sends serialized operators, input/output channels, event-time settings and
  downstream key selectors to workers.
- `TaskManager` creates the local source/operator tasks, input gates and result partitions.
- The Phase 4 `CheckpointCoordinator` owns interval triggers, completion and timeout. Checkpoint
  acknowledgements and completion/abort commands use the master/worker gRPC control plane.

## Data plane and backpressure

Within a worker, `InputGate` holds one bounded queue per upstream channel. Across workers,
`DataTransportService` sends serialized batches over gRPC. The receiver grants credits per
logical channel, avoiding a slow task starving sibling channels on the same socket.

| Exchange | Routing rule |
|---|---|
| `FORWARD` | producer subtask *i* -> consumer subtask *i* |
| `REBALANCE` | round-robin across downstream subtasks |
| `BROADCAST` | every downstream subtask |
| `HASH` | key -> key group -> consumer subtask |

`ResultPartitionWriter` sends control elements only on the channels its task actually feeds. A
forward task must not broadcast a watermark onto unrelated channels: that would make a false
claim about a stream it does not produce.

## Event time and watermarks

Event time enters once at a source, through `TimestampAssigner`, and is retained in every
`StreamRecord`. A source configured with `withEventTime(assigner, outOfOrderness)` uses
`BoundedOutOfOrdernessGenerator`:

```text
watermark = maximum observed event timestamp - allowed out-of-orderness
```

The disorder allowance is a visible latency/completeness trade-off. Each `OperatorTask` uses a
`WatermarkTracker` with one watermark per input. Its clock is the minimum over active channels
and only moves forward.

### Idleness and EOF

A missing distributed message might mean a slow source, congestion, or an idle partition, so
idleness is explicit and in band:

- `Watermark.IDLE` removes a channel from the watermark minimum.
- `Watermark.ACTIVE` is sent before a resumed source record and adds the channel back.
- `withIdleness(Duration.ZERO)` disables detection; a positive duration exposes the
  liveness/correctness trade-off.

A bounded source sends `Watermark.MAX` at EOF. MAX is distinct from idleness: a task forwards it
only after every physical input channel has ended. An idle channel cannot turn one upstream EOF
into a false task end.

## Keyed state and timers

`InMemoryStateBackend` remains useful for local execution and unit tests. Distributed operator
tasks use `RocksDbStateBackend`: named value/list state is stored in RocksDB column families and
indexed by current key. Each operator checkpoint also captures pending event-time timers and
watermark progress; restoring only the accumulator would leave an open session that never fires.

```text
record for member-42
       |
       v
setCurrentKey(member-42)
       +--> ValueState("session")     -> member-42 accumulator
       +--> ValueState("session-end") -> member-42 timer timestamp
       +--> registerEventTimer(end)
```

`TimerService` groups timers by timestamp, deduplicates duplicate `(key, timestamp)` requests,
and fires due timers in timestamp order. It restores the timer key before invoking
`KeyedOperator.onEventTimer`, so state reads in the callback are scoped to its registering key.

### LMS session aggregation

`SessionAggregator` retains an incremental `SessionAccumulator` and end timestamp per member,
not a list of events. Extending a session registers a later timer and leaves the old timer in the
service; that old callback compares with the stored end and becomes harmless. The matching timer
emits its `SessionRow` and clears both state values.

A session is defined by event time. An event after the current end closes/resets the prior session
instead of being merged just because the watermark has not advanced. A record too old to merge
with the current open session is counted and dropped; Phase 3 intentionally has no
allowed-lateness reopening policy.

`FileReplaySource` provides bounded JSON Lines input for deterministic replay. Stable line-hash
partitioning gives the same source ownership on each replay with the same parallelism.

## Phase 5 interval join and multi-input streams

`engine-api` exposes `Either<L,R>` to tag records from two logical inputs, `JoinFunction` to map a
matching pair, and `IntervalJoinOperator<K,L,R,O>` as a keyed event-time operator. The LMS job
tags result clicks with `Either.left` and borrows with `Either.right`, then keys both by
`ConversionKey(memberId, catalogItemId)` before the join.

The LMS bounds are `[0, 30 minutes]` on `borrowTime - resultClickTime`, inclusive. Every arrival
checks the opposite keyed buffer before being retained itself. This supports either arrival order
and emits each matching pair once, when its second record arrives. Both sides use their own keyed
`ListState` (`interval-join-left` and `interval-join-right`), with `left-buffer-size` and
`right-buffer-size` gauges reporting the two retained populations independently.

Each buffered record registers an event-time cleanup timer. For the LMS forward interval, a left
record is removed only when `leftTime + upperBound < watermark`; a right record is removed only
when `rightTime < watermark - upperBound`. Equality is still retained, so a match at exactly zero
or exactly 30 minutes remains possible. Timers run under their original `ConversionKey`, and the
callback checks the current watermark before filtering both state lists.

`DataStream.union` creates a transparent transform with two upstream ids, forwards record values
without changing timestamps, and sets `REBALANCE` on its inputs. Fan-in cannot use `FORWARD`
because the branches may have different parallelisms and no unique subtask mapping. Union also
forms a multi-input task boundary: each upstream operator/subtask has a distinct `InputGate`
channel, so the runtime can track each branch's watermark and checkpoint barrier independently.

When one producer fans out to session and conversion branches, `FanOutOutput` delegates to one
edge-specific routed output per downstream. The edges therefore preserve independent routing and
key selectors. The union input remains a single physical input to the interval operator after the
two branches have converged.

The borrow side has its own deterministic fixture in `demos/fixtures/borrows.jsonl`, published by
`demos/seed-borrows.sh`. Seed it before or after clicks. If one input advances much faster, records
from that side remain in its keyed list until the minimum input watermark passes their cleanup
horizon. The slower stream therefore increases the fast side's buffer, a direct demonstration of
the slower-stream effect.

## Checkpoint protocol and fault tolerance

The master triggers checkpoints on a configurable interval (10 seconds by default) and allows a
configurable timeout (30 seconds by default). The coordinator sends a trigger only to source tasks.
`MasterBootstrap` reads the interval, timeout, restart-attempt count and restart delay from
environment overrides used by the cluster launcher.
Each source takes a snapshot on its own task thread at a record boundary, then emits the barrier
into the ordinary ordered data channels. This gives every downstream task the same stream cut
without asking unrelated tasks to snapshot at arbitrary instants.

For a task with multiple physical input channels, `BarrierAligner` blocks a channel as soon as its
barrier arrives. The task keeps consuming the other channels, whose pre-barrier records still
belong to the snapshot. When all channels have delivered the matching barrier, the task follows
this order:

```text
snapshot state + timers + watermark progress
             ↓
forward barrier downstream
             ↓
acknowledge snapshot to the master
             ↓
unblock channels and process buffered post-barrier records
```

Forwarding before acknowledgement matters: the coordinator can complete only when every physical
task in the execution plan has acknowledged. Single-input tasks skip channel blocking and take the
same snapshot/forward/ack path directly. If a checkpoint times out, the master sends an abort to
workers so any channels held by that incomplete alignment are released.

Each worker first writes the task snapshot to local files. `RocksDbStateBackend` snapshots its
database; the task envelope records its state handle together with pending timer data and event-time
progress. Sources add connector position and watermark-generator state. `KafkaSource` assigns
partitions explicitly and snapshots the next offsets without committing them to Kafka. On restore,
the new source seeks to the checkpointed offsets before polling resumes.

The worker packages the entire task checkpoint directory as a ZIP and publishes it through
`CheckpointStorage`. In a cluster, `MinioCheckpointStorage` stores the immutable archive in the
configured MinIO bucket. File handles nested inside the archive are relative paths, not paths from
the producing worker's disk. On recovery, any worker can download and materialize the archive into
its own checkpoint directory. A filesystem archive implementation supports tests and local use.

The coordinator expects one acknowledgement per chain-group subtask. An acknowledgement carries a
state handle, alignment time and state size. Only when every expected task has acknowledged does
the coordinator persist the completed checkpoint pointer and all task handles to etcd; only after
that durable write does it send the completion notification. Incomplete timed-out checkpoints are
aborted and never become recovery points.

On worker failure, the job moves to `FAILING`, all tasks are cancelled, and the master loads the
latest completed checkpoint. It then moves to `RESTARTING`, waits the fixed delay, and redeploys the
entire graph with each task's corresponding handle. The default policy permits three attempts,
one second apart; without a completed checkpoint or after attempts are exhausted, the job becomes
`FAILED`. Workers declared dead are quarantined from scheduling while their etcd lease may still
exist. Fresh registration makes a worker eligible again.

Workers reopen their heartbeat stream after a master interruption. The first beat seeds the new
master's in-memory failure detector, while etcd supplies the durable worker addresses and recovered
execution plan. For bounded jobs, the master retains the latest task statuses and marks the job
`FINISHED` only after every physical task reports completion; checkpoint scheduling stops then.

### Phase 4 runtime evidence

`DistributedCheckpointRecoveryIT` supplies the automated process-level acceptance proof; run it
with `./gradlew :lms-job:integrationTest`. Testcontainers runs etcd and MinIO while the test
starts a real master and three worker child JVMs. The five-task bounded job completes a checkpoint
while the sink has emitted zero bytes. The test then force-kills the session owner selected by
`KeyGroupAssigner`, verifies the tasks are reassigned without the dead worker, and waits for the
bounded job to reach `FINISHED`. Output is byte-identical to a clean baseline run.

The live three-worker recovery run used 10 tasks. Checkpoints 1–5 completed with a handle for each
task in etcd and MinIO. After worker-2 was killed, the detector declared it dead after three missed
one-second beats (about 3.47 seconds). The master identified `clicks:1`, `sessions:0`, and
`sessions:3` as lost tasks, cancelled the full graph, and restarted attempt 1/3 from checkpoint 5
on worker-1 and worker-3. The new assignments excluded worker-2; checkpoint 6 and later again
completed with 10 handles, with `minio://` state handles including source and operator envelopes.

The process-level test verifies bounded completion and byte equality through real master/worker
processes. The separate `SessionPipelineRecoveryAcceptanceTest` compares the recovered
source/session/sink pipeline with a clean fixed replay, while the MinIO integration test deletes
the producer-side checkpoint directory before restoring the archive on a different filesystem root.

## Connectors

- **KafkaSource (implemented):** pull-based LMS input with explicit partition assignment and
  checkpointed next offsets. Restore seeks to those offsets; the broker's committed offsets are
  not the recovery position.
- **ConsoleSink (implemented):** a normal `Operator<T, Void>` for visible output.
- **FileReplaySource (implemented):** bounded fixture source for deterministic tests/demos.
- **IcebergSink (planned, Phase 6):** will pre-commit files into state and atomically append them
  after a completed checkpoint. Its commit must be idempotent after a crash.

## Observability

Implemented metrics include per-subtask record counts and the latest checkpoint id, snapshot
duration, state size in bytes, and barrier-alignment time. Worker heartbeats carry these samples,
and the master retains the latest status per physical task; Phase 7 exposes that registry through
scrape endpoints and dashboards.

**Planned Phase 7:** REST status, Prometheus endpoints and Grafana panels for source lag,
checkpoint duration/alignment, records-in skew and state size. Scripted fixture demos will cover
worker loss mid-window, master loss, a late event and a hot key.

## Deliberate limitations

There is one master, fixed parallelism, no savepoints, no dynamic rescaling, no SQL layer, and no
security/multi-tenancy/resource isolation. Checkpoint recovery and interval joining are
implemented, but the console sinks are not transactional; exactly-once external output awaits the
Phase 6 Iceberg commit protocol. Operational APIs remain planned. These omissions are visible so
the code shows which production-system mechanism solves each problem.
