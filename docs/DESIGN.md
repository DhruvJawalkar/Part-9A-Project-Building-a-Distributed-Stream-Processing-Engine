# Engine design

This living design document records what is implemented through **Phase 3** and labels later
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
| 4 | **Planned** | Coordinated checkpoints, barrier alignment and whole-job recovery |
| 5 | **Planned** | Event-time interval join |
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

## Computation DAG

`JobGraph` is a serializable logical DAG. Nodes have stable, user-supplied identifiers; edges
describe how a consumer receives input. Validation rejects cycles, unreachable operators, and
non-positive parallelism.

```text
source("clicks") -- FORWARD --> filter("drop-bots")
                                      |
                                      | HASH by memberId
                                      v
                              process("sessions") -- REBALANCE --> sink("console")
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
- Through Phase 3, a worker loss fails the job. Heap state and source offsets do not yet form a
  coordinated recovery point.

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

`InMemoryStateBackend` implements `StateBackend` for Phase 3. It keeps named value/list state,
each indexed by current key. It can produce local snapshot handles, but those handles are not yet
coordinated across a job.

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

## Fault tolerance and recovery

### Implemented through Phase 3

The master detects a lost worker and fails the job. Local heap state is not reused, and Kafka
offsets remain connector-managed. The end-to-end guarantee is therefore at-least-once, not
exactly-once.

### Planned Phase 4: checkpoints and recovery

`CheckpointCoordinator` will inject barriers at sources. `BarrierAligner` will block an input
when its barrier arrives while still consuming unblocked channels; once all checkpoint barriers
arrive, the task will snapshot state, forward its barrier, acknowledge, then drain buffered data.
Single-input tasks do not need alignment.

The coordinator will complete only after every task acknowledges, persist the completed pointer
in etcd, and notify sinks. Recovery will cancel and redeploy the whole job from the latest
checkpoint, restoring state and seeking sources to checkpoint offsets. Restarting every task is
intentional: one task alone could combine incompatible prefixes of a distributed cut.

## Connectors

- **KafkaSource (implemented):** pull-based LMS input; Kafka owns offsets until planned
  checkpoint integration takes them into snapshots.
- **ConsoleSink (implemented):** a normal `Operator<T, Void>` for visible output.
- **FileReplaySource (implemented):** bounded fixture source for deterministic tests/demos.
- **IcebergSink (planned, Phase 6):** will pre-commit files into state and atomically append them
  after a completed checkpoint. Its commit must be idempotent after a crash.

## Observability

Implemented metrics include per-subtask record counts; task logs and master/worker heartbeats
show lifecycle and liveness.

**Planned Phase 4:** checkpoint duration, state size and barrier-alignment time in acknowledgements.

**Planned Phase 7:** REST status, Prometheus endpoints and Grafana panels for source lag,
checkpoint duration/alignment, records-in skew and state size. Scripted fixture demos will cover
worker loss mid-window, master loss, a late event and a hot key.

## Deliberate limitations

There is one master, fixed parallelism, no savepoints, no dynamic rescaling, no SQL layer, and no
security/multi-tenancy/resource isolation. Checkpointing, recovery, joins, Iceberg and
operational APIs are planned rather than implicit. These omissions are visible so the code shows
which production-system mechanism solves each problem.
