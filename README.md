# Distributed Stream Processing Engine

A teaching implementation of a distributed stream processing engine in Java 21. It runs master
and worker processes, routes records over gRPC, processes keyed streams in event time, and recovers
the whole job from coordinated checkpoints. It also includes a keyed event-time interval join and
checkpoint-transactional Iceberg output.

It accompanies **Part 9A: Stream Processing Fundamentals** of the *Developing Intuition on
Building Blocks — Systems Design* series. The article explains how an engine like this works;
this repository is a working one, built phase by phase so that each mechanism is demonstrable
on its own before the next is added. The supplied article is available as
[Part 9A: Stream Processing Fundamentals (PDF)](docs/Part9A-article-StreamProcessingFundamentals.pdf).

The design rationale, interfaces and algorithm sketches live in
[`docs/Part9A_Project_Companion.pdf`](docs/Part9A_Project_Companion.pdf).
[`CLAUDE.md`](CLAUDE.md) is the authoritative build instruction.
[`docs/DESIGN.md`](docs/DESIGN.md) is the living implementation design.
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) follows a record from the job DAG through
scheduling, event time, checkpointing, state, transactional output, and recovery.
[`docs/PHASE7-ACCEPTANCE.md`](docs/PHASE7-ACCEPTANCE.md) records final verification and the
commands for the manual/demo confirmation pass.
[`docs/BUG-LOG.md`](docs/BUG-LOG.md) records every bug found while building this, and why most of
them produced no error at all.

## The one rule

This is a teaching engine. Its purpose is to make the mechanisms **legible**, not to compete
with Flink. Wherever a clever optimisation and an obvious implementation disagree, the obvious
one wins. You should be able to open `BarrierAligner.java` and watch Chandy-Lamport happen.

Every shortcut is named rather than quietly implied. Knowing exactly where a teaching
implementation stops is part of understanding what production engines do for you.

---

## Status

| Phase | What it adds | State |
|---|---|---|
| **1** | Single-process engine: Kafka → filter → console | **Complete** |
| **2** | Master and worker processes, gRPC transport, etcd | **Complete** |
| **3** | Event time, watermarks, keyed state, session windows | **Complete** |
| **4** | Checkpointing, barrier alignment, portable state, recovery | **Complete** |
| **5** | Two-stream event-time interval join and conversion branch | **Complete** |
| **6** | Transactional Iceberg sink, REST catalog and MinIO demo surface | **Complete** |
| **7** | REST/Prometheus status, Grafana, Compose stack and four reproducible demos | **Complete — verified** |

---

## Quickstart

Requires **JDK 21+** and **Docker**.

```bash
docker compose up -d --build
```

That command starts Kafka, etcd, MinIO, the Iceberg REST catalog, one master, three workers,
Prometheus, and Grafana. One-shot bootstrap services create both Iceberg tables, submit the LMS
job after all three workers register, and publish the deterministic click and borrow fixtures.
With preserved volumes, bootstrap reuses a nonterminal LMS job and does not automatically replay
fixtures into nonempty input topics. Use a fresh Compose project/volumes for a clean demonstration;
`docker compose down` preserves data, while `down -v` is an explicit destructive reset.
The status API is at <http://localhost:18080/jobs>, Prometheus at <http://localhost:9090>, and
Grafana at <http://localhost:13000> (`admin` / `admin`). The deliberately offset host ports avoid
collisions with common local web-development ports; `MASTER_STATUS_HOST_PORT`,
`WORKER1_METRICS_HOST_PORT` through `WORKER3_METRICS_HOST_PORT`, and `GRAFANA_HOST_PORT` can
override them. The provisioned dashboard has exactly the
four signals used in the article: source lag, checkpoint duration/alignment, records-in by
subtask, and state size by subtask.

The host-launched workflow remains useful while changing Java code:

```bash
docker compose up -d kafka etcd minio minio-init iceberg-rest
./demos/run-cluster.sh
./gradlew :lms-job:submitToCluster
./demos/seed-clicks.sh
./demos/seed-borrows.sh
./demos/seed-watermark-progress.sh
```

The job writes two unpartitioned Iceberg tables: `lms.analytics.browse_sessions` for closed
`SessionRow` values and `lms.analytics.click_conversions` for a `RESULT_CLICK` followed by a
matching borrow within 30 event-time minutes. Both input fixtures are deterministic, and
click/borrow arrival order does not change the join result. `run-cluster.sh` initializes the
schema as well, so the explicit init step can be omitted when using that launcher alone. The
final progress seed advances every input partition beyond the short fixture's session gaps;
wall-clock silence alone never closes an event-time window. Compose appends these clock records
automatically. Kafka
partition positions are owned by the engine: the next offset is captured in each source
checkpoint, and recovery seeks to that offset without relying on broker-committed consumer-group
positions. Run the full unit suite with `./gradlew test`. The focused Phase 3 proof is:

```bash
./gradlew :engine-runtime:test :lms-job:test :engine-connectors:test
```

It covers bounded out-of-orderness, the silent-partition/idleness regression, keyed routing and
state, timer key restoration, session extension, stale-timer suppression, state clearing, and
deterministic file replay. Phase 4 checkpoint unit, integration, and recovery acceptance tests
run through Gradle; the Docker-backed MinIO check is
`./gradlew :engine-worker:integrationTest`. The Phase 5 join tests run with `./gradlew :engine-api:test`
and the LMS fixture replay with `./gradlew :lms-job:test`.

Keep the cluster running while you inspect the session and conversion rows in the two Iceberg
tables. `demos/logs/` contains master/worker operational logs rather than result rows.
When finished, use `docker compose down` to retain the demo volumes, or `docker compose down -v`
for a clean replay. For the host workflow, stop workers first with `./demos/run-cluster.sh stop`.

### A note on output ordering

Session rows are not globally ordered, and that is not a bug. The `by-member` hash edge keeps each
member on one session subtask, but different member keys run independently before the singleton
Iceberg writer. Ordering survives within one keyed partition; a global result ordering would
require an explicit downstream coordination point and is not part of this engine.

### If `./gradlew` fails with a bare version number

Gradle 8.14 runs on JDK 17–24 and refuses anything newer with an unhelpful message. This repo
pins the launcher JDK in `gradle.properties` via `org.gradle.java.home`; that line is
machine-local and should be removed or repointed elsewhere. The **build** always targets Java 21
through a toolchain, independent of whichever JDK launched Gradle.

---

## Module layout

The dependency direction is strict and enforced by Gradle rather than by convention.

```
engine-api/          No runtime dependencies, ever. StreamElement, Operator, KeyedOperator,
                     OperatorContext, state interfaces, JobGraph builder, ExchangeStrategy,
                     KeyGroupAssigner, Either, DataStream.union, IntervalJoinOperator.
engine-rpc/          No engine dependencies. The .proto wire contracts and generated stubs.
engine-metadata/     No engine dependencies. MetadataStore, etcd and in-memory implementations.
engine-runtime/      → engine-api, engine-rpc
                     Task run loop, event time, keyed state and timers, input gates, transport,
                     FanOutOutput and serialization.
engine-master/       → engine-api, engine-rpc, engine-metadata, engine-runtime
                     JobMaster, ExecutionGraph compiler, TaskTracker, JobClient.
engine-worker/       → engine-api, engine-rpc, engine-metadata, engine-runtime
                     Worker bootstrap, TaskManager, gRPC servers, heartbeat client.
engine-connectors/   → engine-api, engine-runtime
                     KafkaSource, IcebergSink, ConsoleSink, FileReplaySource.
lms-job/             → engine-api, engine-connectors
                     The LMS clickstream job, and its main().
```

`engine-rpc` and `engine-metadata` exist for the same reason: the control plane has two ends.
`engine-master` serves `MasterService` and calls `WorkerService`; `engine-worker` does the
reverse. Both need the same wire types and the same view of etcd, and neither should depend on
the other. A module holding only contracts, depending on no engine module, points inward from
both sides and leaves the dependency direction intact.

`engine-api` having **zero** dependencies is a hard constraint, verifiable with
`./gradlew :engine-api:dependencies --configuration runtimeClasspath`. It is the surface a job
author writes against; anything added there lands on their classpath.

`lms-job` cannot see `engine-runtime`. `engine-connectors` declares the runtime as
`implementation` rather than `api`, so runtime internals never reach the job's compile
classpath. If a job class fails to compile because it cannot see a runtime type, that is the
rule working. (This is an amendment to the original "engine-api only" rule; see CLAUDE.md §3.)

---

## What Phase 1 built

- **`engine-api` in full** — the record envelope (`StreamElement` sealed over `StreamRecord`,
  `Watermark`, `CheckpointBarrier`), the operator interfaces, keyed state and state backend
  interfaces, metrics, the `JobGraph` builder with validation, and `KeyGroupAssigner`.
- **The task run loop**, already switching exhaustively over all three element kinds. The
  watermark and barrier branches are stubs with the phase that fills them named in a comment.
- **`LocalJobExecutor`** — one thread per subtask, bounded queues between them. The bounded
  queue *is* the backpressure mechanism.
- **`KafkaSource`** reading JSON, and **`ConsoleSink`**.
- **The LMS job** — `ClickEvent`, `BorrowEvent`, `BotFilter`, and a `main()`.
- **50 tests**, including the `JobGraph` validation rules and every property `KeyGroupAssigner`
  has to hold.

### Two bugs worth knowing about

Both were found by running the thing, and both are pinned by tests. Full write-ups, along with
everything found in later phases, are in [`docs/BUG-LOG.md`](docs/BUG-LOG.md).

**Every subtask needs its own operator instance.** A job graph holds one operator object. Run
it four ways and four threads share it — which, when the operator is a `KafkaSource`, means
four threads sharing one `KafkaConsumer`. It fails loudly there and silently almost everywhere
else. `TaskInstances` gives each subtask a private copy by serializing and deserializing it,
which is exactly what shipping a task to a worker will do in Phase 2. A distributed engine never
sees this problem; a local executor that did not imitate it would be lying about the engine.

**A forward edge broadcasts to one channel, not all of them.** Under a forward exchange, subtask
*i* feeds only subtask *i*. Broadcasting a watermark to every downstream queue would be a claim
about a stream this task does not produce. A checkpoint barrier sent that way would be worse:
it would arrive on a channel that will never deliver the records it is meant to separate.

---

## What Phase 2 built

Run a real cluster:

```bash
docker compose up -d                  # Kafka and etcd
./demos/run-cluster.sh                # master on :7000, three workers on :7001-7003
./demos/seed-clicks.sh                # publish the fixture
./gradlew :lms-job:submitToCluster    # submit to the master
./demos/run-cluster.sh stop
```

Worker logs land in `demos/logs/`. The job compiles to **6 tasks across all 3 workers**, and the
15 surviving records are printed by sink tasks on machines that did not read them from Kafka.

- **Wire contracts** — `MasterService`, `WorkerService`, `DataTransportService`, per PDF §6.
- **`ExecutionGraphCompiler`** — expands operators to subtasks, fuses chains, assigns vertical
  slices round-robin.
- **`ChainBuilder`** — the four conditions for fusing two operators into one thread.
- **`EtcdMetadataStore`** — the PDF §8.3 key layout, worker leases, prefix watches.
- **`TaskTracker`** — 1s heartbeats, dead after 3 missed.
- **Transport** — `InputGate` with one queue per channel, batching on size or
  `bufferTimeoutMs`, and credit-based flow control.
- **94 unit tests** plus 7 etcd integration tests (`./gradlew :engine-metadata:integrationTest`).

### Four bugs worth knowing about

All four were found by running the cluster, and all four are now pinned by tests. Three of them
produced no error at all -- see [`docs/BUG-LOG.md`](docs/BUG-LOG.md) for the full write-ups and
for why that pattern matters.

**A credit deadlock that looked like nothing happening.** A sender starts at zero credit and
waits for permission. The receiver granted credit only *after* receiving a buffer — so it was
waiting for a buffer the sender was not allowed to send. Every process healthy, job `RUNNING`, no
errors, no records. The opening grant must be sent when the stream opens, before any data.

**gRPC `Context` cancellation killing the data streams.** The record streams are opened while
handling the master's `DeployTask` call, so they inherited that server call's `Context` — which
is cancelled the instant the handler returns. Long-lived connections must be opened under
`Context.ROOT`.

**`OperatorChain` zeroed event time.** `out.collect(value)` deliberately takes no timestamp, and
the chain never seeded it from the incoming record, so every chained operator saw the epoch.
Nothing failed; windows downstream would simply have been wrong. Caught by a test, not by a run.

**The client compiled and deployed the job itself.** It worked, until a worker died and the
master had never been told the job existed. Submission now goes through `MasterService.SubmitJob`
— whoever must react to failure has to be the one that knows what is running.

---

## What Phase 3 built

Phase 3 makes the runtime care about event time. The LMS job is now:

```text
Kafka clicks → drop-bots → keyBy(memberId) → SessionAggregator → console
```

`keyBy` is an edge property, not an operator. `ResultPartitionWriter` hashes the key through
`KeyGroupAssigner`, so all records for one member reach the same `SessionAggregator` subtask.
That task sets the current key before calling user code; the operator's `ValueState` handles and
event-time timer registrations are therefore automatically scoped to that member.

- **Source watermark generation** — `BoundedOutOfOrdernessGenerator` emits the greatest event
  time seen minus the configured disorder allowance. `withIdleness(Duration)` makes a silent
  source subtask emit an in-band idle status; `Duration.ZERO` disables that detection.
- **Watermark propagation** — `WatermarkTracker` retains one watermark per input channel and
  advances only to the minimum across active channels. It never moves a task clock backwards.
  An idle channel is excluded, and becomes active again before its resumed record is sent.
- **Timers and state** — `TimerService` fires due timers in timestamp order and restores the
  timer's key into `InMemoryStateBackend` before the callback. State handles acquired in
  `open()` resolve the current key on every access.
- **Sessions** — `SessionAggregator` retains a running accumulator and end timestamp per member,
  not a list of input events. Extending a session registers a later timer; when an old timer
  fires it is ignored by comparing against the stored end. An event beyond the current end
  closes/resets the prior session, and an unmergeably late event is counted/dropped; the matching
  timer emits and clears both entries.
- **Reproducible input** — `FileReplaySource` reads a JSON Lines fixture as a bounded,
  deterministically partitioned source for tests and future demos.

Two intentionally visible boundaries matter. A bounded source emits `Watermark.MAX` only after
it has reached EOF, and an operator task forwards MAX only after every input channel has ended;
an idle channel must never turn one channel's end into a false whole-job end. Also, keyed
operators form a chain boundary today: each keyed task owns one key context, state backend and
timer service. This keeps scope obvious until state names are namespaced per operator.

---

## What Phase 4 built

Phase 4 gives the engine a consistent recovery point across source positions, keyed state, timers,
and every task in the computation DAG. The default checkpoint interval is 10 seconds, with a
30-second timeout; the fixed-delay restart policy allows three attempts, one second apart.
The cluster launcher accepts `CHECKPOINT_INTERVAL_MS`, `CHECKPOINT_TIMEOUT_MS`,
`RESTART_MAX_ATTEMPTS`, and `RESTART_DELAY_MS` as environment overrides.

- **Coordinator and protocol** — `CheckpointCoordinator` injects barriers at source tasks only,
  accepts one acknowledgement per physical task, and publishes a completed checkpoint pointer to
  etcd only after every task has acknowledged. It then sends the completion notification. Timed-out
  checkpoints are aborted, and workers release channels held by incomplete alignments.
- **Barrier alignment** — a multi-input task blocks each channel after its barrier arrives, while
  continuing to read channels that have not reached the same barrier. It snapshots only after all
  inputs have arrived, forwards the barrier, acknowledges, and then releases buffered post-barrier
  records. Single-input tasks take the direct path without blocking their only input.
- **Recoverable state** — `RocksDbStateBackend` stores keyed value and list state, with pending
  event-time timers and watermark progress captured in the task checkpoint envelope. Source
  snapshots include Kafka topic-partition offsets and source watermark-generator progress; Kafka
  offsets are restored with `seek()` and are not committed as the recovery position.
- **Portable checkpoint archives** — each task stages a local snapshot, archives its complete
  checkpoint directory, and publishes the archive to MinIO. etcd stores task handles only for a
  fully completed checkpoint. Nested paths are relative to the archive, so recovery can download
  and restore on a different worker filesystem. A filesystem-backed archive store is available
  for local tests.
- **Whole-job recovery** — on worker loss or master restart, the master cancels every task, loads
  the latest completed checkpoint from etcd, and redeploys the full DAG with each task's state handle.
  Rewinding after a master restart also fences any checkpoint id that was in flight only in the old
  master's memory. A worker declared
  dead is quarantined from scheduling until it registers afresh. The job moves through
  `FAILING → RESTARTING → RUNNING`, or becomes `FAILED` when no checkpoint exists or attempts are
  exhausted.
- **Checkpoint metrics** — acknowledgements and worker heartbeats carry checkpoint id, snapshot
  duration, state size in bytes, and barrier-alignment time per task. The master retains the
  latest sample for the Phase 7 status API.
- **Lifecycle continuity** — workers reconnect their heartbeat streams after a master restart,
  and bounded jobs become `FINISHED` only after every physical task has ended.

### Phase 4 recovery evidence

`DistributedCheckpointRecoveryIT` is the automated process-level proof; run it with
`./gradlew :lms-job:integrationTest`. Testcontainers provides etcd and MinIO, and the test launches
a real master and three worker child JVMs. Its five-task bounded job completes a checkpoint while
the sink has emitted zero bytes. The test force-kills the session owner selected by
`KeyGroupAssigner`, verifies that recovery reassigns its work without the dead worker, and waits
for the job to reach `FINISHED`. Its output is byte-identical to a clean baseline run.

The separate manual three-worker run scheduled 10 tasks. Checkpoints 1–5 completed with 10 task
handles each published to MinIO and recorded in etcd. After worker-2 was force-killed, the
heartbeat detector declared it dead after three missed beats (about 3.47 seconds); the master
recorded the three lost tasks (`clicks:1`, `sessions:0`, `sessions:3`), cancelled the whole job,
and restarted attempt 1/3
from checkpoint 5 on worker-1 and worker-3. The recovered assignments excluded worker-2, and
checkpoints 6 onward again completed with all 10 handles. The handles used `minio://` URIs and
included source-position and operator-state envelopes.

The focused `SessionPipelineRecoveryAcceptanceTest` also compares recovered source/session/sink
execution with a clean fixed replay. The MinIO integration test restores an archive after deleting
the producer's local checkpoint directory.

---

## What Phase 5 built

The LMS job now has two event-time branches. Clean clicks still feed session aggregation; result
clicks also join with borrows keyed by `ConversionKey(memberId, catalogItemId)`:

```text
clicks → drop-bots ──┬── keyBy(memberId) → SessionAggregator → browse_sessions
                     └── RESULT_CLICK → Either.left ───────────┐
borrows ───────────────────────────────→ Either.right ─────────┤
                                                               └─ union(REBALANCE)
                                                                  → keyBy(ConversionKey)
                                                                  → IntervalJoinOperator
                                                                  → click_conversions
```

`engine-api` exposes the sealed `Either<L,R>` tag, `JoinFunction`, and keyed
`IntervalJoinOperator<K,L,R,O>`. Each tagged arrival checks the opposite side's keyed `ListState`,
emits one row for every match, then buffers itself. Thus either stream can arrive first and each
matching pair emits exactly once when its second member arrives. The LMS interval is inclusive:
`0 <= borrowTime - resultClickTime <= 30 minutes`.

The join retains separate left and right `ListState`s per `ConversionKey`. Event-time timers remove
left records only when `leftTime + upperBound < watermark`, and right records only when
`rightTime < watermark - upperBound` for this forward 0..30-minute interval. These strict
inequalities keep matches at either inclusive endpoint available; cleanup occurs only after the
watermark has passed the final possible match time. `left-buffer-size` and `right-buffer-size`
gauges expose the retained records independently.

`DataStream.union` creates a transparent fan-in node, preserves record timestamps, and uses
`REBALANCE`: two input branches need not have a one-to-one subtask correspondence, so `FORWARD`
would be ambiguous. A union is a multi-input task boundary, with a distinct `InputGate` channel
for each upstream operator/subtask. Watermarks and checkpoint barriers therefore remain associated
with their own branch. At a producer with multiple downstream branches, runtime `FanOutOutput`
hands each edge its own routed output so the session and conversion paths keep their own exchange
strategy and key selector.

`demos/fixtures/borrows.jsonl` and `demos/seed-borrows.sh` provide deterministic conversion input.
Seed the click and borrow fixtures in either order; the keyed join buffers whichever side arrives
first. The fixture includes a borrow just beyond the 30-minute upper bound to make the eviction
boundary observable.

---

## What Phase 6 built

Phase 6 makes the external output boundary transactional for unpartitioned Iceberg tables. The
engine's optional `CheckpointListener` lifecycle is deliberately small:

- After a task aligns a barrier, `preCommit(checkpointId)` runs on the task thread before the
  keyed-state snapshot. Its serializable return value is stored in the task checkpoint envelope
  beside keyed state, timers, watermarks and source position. On recovery, the operator is opened
  first and then receives that saved state.
- Once every physical task has acknowledged, the master writes the completed-checkpoint pointer
  and all task handles to etcd. Only after that durable write does it notify sink tasks. Completion
  and abort callbacks are queued and executed by each operator task's own run loop, so a gRPC
  handler never calls user code concurrently with record processing.
- Recovery redeploys the whole graph from the durable pointer. It replays the completion callback
  for that checkpoint because a master can fail after a sink commit but before the callback has
  been observed. The sink's current-table check makes that replay idempotent.

`IcebergSink` owns one Parquet writer per checkpoint interval. `preCommit` closes the writer and
stores its path, size and row count as pending file state, then opens the next interval's writer.
Those files are not table rows yet: only `notifyCheckpointComplete` appends the missing paths in
one Iceberg metadata commit. Before appending, the sink refreshes the current table and scans
reachable data-file paths, so a repeated completion cannot append a file twice. An aborted
checkpoint or a worker lost after closing a writer leaves an object-store orphan; it is absent from
every Iceberg snapshot and therefore contributes no visible rows. The next worker replays source
records from the last completed checkpoint and publishes one replacement file, preserving each
logical row exactly once.

The LMS graph creates one singleton sink subtask for each table, making each checkpoint interval a
single table append. The exact table names are `lms.analytics.browse_sessions` and
`lms.analytics.click_conversions`. Workers construct the REST catalog and S3 file IO from
serializable settings rather than shipping a catalog client in the job. Compose starts Kafka,
etcd, MinIO and `apache/iceberg-rest-fixture`; MinIO provides the `warehouse` bucket and
checkpoint bucket, while `demos/iceberg/init-schema.sh` idempotently bootstraps the namespace and
both table schemas. The host-launched workers use `localhost:8181` for the catalog and
`localhost:9000` for MinIO; the catalog container uses `minio:9000` internally.

### Phase 6 acceptance evidence

`IcebergSinkAcceptanceTest` exercises the transaction protocol with Iceberg's in-memory catalog
and local file IO. It verifies that Parquet files and rows remain invisible to a table scan until
checkpoint completion, that completion replay creates no duplicate snapshot or row, and that a
simulated lost sink leaves the old closed file as an unreachable orphan while a restored sink
exposes each replayed row once. Its deterministic six-event cadence comparison is:

| Checkpoint cadence | First visibility | Data files | Snapshots | Rows |
|---|---:|---:|---:|---:|
| Short | 100 ms | 3 | 3 | 6 |
| Long | 500 ms | 1 | 1 | 6 |

The shorter cadence makes rows visible sooner at the cost of more small files. This Phase 6 sink
supports unpartitioned tables only; opening a partitioned destination fails explicitly rather than
silently producing incorrect file metadata. The Compose REST-catalog/MinIO surface is executable
demo infrastructure. The opt-in `IcebergRestMinioSmokeTest` verifies the real REST catalog,
`S3FileIO`, MinIO write and checkpoint commit path. Phase 7 adds the real process-kill proof in
`DistributedCheckpointRecoveryIT.killedSinkWorkerLeavesOrphanAndRecoversExactlyOnceIcebergRows`:
a sink-owning worker is force-killed after closing durable, still-invisible Parquet files but
before its checkpoint ACK. The restored pipeline commits exactly the clean table's two session
rows once; the old files remain present and absent from reachable snapshots. This comparison uses
a canonical `SessionRow` string column in two real REST/MinIO tables, not the production LMS
table schema. The production schema is verified separately by connector tests and the full stack.

---

## What Phase 7 built

Phase 7 makes the earlier correctness claims observable and reproducible. The master exposes a
small framework-free HTTP API on port 8080:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/jobs` | List job id, name, state, and start time |
| `GET` | `/jobs/{jobId}` | State, logical vertices, and restart count |
| `GET` | `/jobs/{jobId}/tasks` | Worker placement, records, watermark, queue occupancy, and backpressure per subtask |
| `GET` | `/jobs/{jobId}/checkpoints` | Interval, completed/failed counts, and checkpoint history |
| `GET` | `/jobs/{jobId}/lag` | Kafka lag by owned partition and maximum lag time |
| `POST` | `/jobs/{jobId}/cancel` | Persist and execute an explicit cancellation |
| `GET` | `/metrics` | Prometheus exposition for the master |

Each worker also serves `/metrics` on its metrics port. Task series retain `job`, `operator`,
`subtask`, and `worker` labels so skew cannot disappear into a cluster-wide aggregate. Queue
occupancy and `stream_engine_backpressured` reflect the real bounded `InputGate`; checkpoint
duration, alignment, and state bytes come from the latest task acknowledgement. Kafka source lag
is calculated from each source's owned partition positions and broker end offsets.
Prometheus honors the engine's `job` UUID label. Grafana selects the worker series so a task
does not appear twice through the worker scrape and the master's heartbeat projection.

The nullable/status-lifetime boundaries are intentional. `startedAt` is the current master's
admission timestamp and is `null` after master recovery because submission time is not persisted.
Task `watermark` is `null` until the task has established event-time progress; heartbeats then
carry its real, checkpoint-restored watermark (including a valid value of zero).
Checkpoint counters/history are current-master operational history; etcd retains the
latest completed recovery point, not an observability journal. `maxLagMillis` is `null` because
Kafka offsets alone cannot reveal the timestamp of an unread record without fetching it.

The default `docker-compose.yml` is the executable deployment diagram. It builds one Java 21
runtime image and runs the same artifacts as the host launcher. The LMS user-code libraries are
present in the image because this teaching engine names a job classpath at process startup rather
than uploading a job JAR. Schema initialization, job submission, and fixture publication are
one-shot services; Prometheus and Grafana are provisioned from files under `observability/`.

### Four reproducible demonstrations

Every script prints the signal to watch, the claim it proves, and the matching article section
before it runs. The scripts use checked-in fixtures and run assertion-backed acceptance harnesses
that fail when the claimed outcome is absent. Docker-backed demos launch their own isolated
dependencies and master/worker JVMs; they do not kill your running Compose cluster.

| Demo | Run | What to watch | What it proves |
|---|---|---|---|
| Worker dies mid-window | `./demos/demo-1-worker-loss.sh` | Session-owner loss rewinds every task; sink-owner loss leaves an orphan; recovered bytes and Iceberg rows equal clean runs | A checkpoint is one distributed cut across offsets, state, timers, and output (§§10.1–10.3, 11) |
| Master dies | `./demos/demo-2-master-loss.sh` | Workers progress while the master is force-killed; a fresh master restores every task from etcd/MinIO | Durable metadata, not master RAM, owns recovery (§§7, 10.3) |
| Late event | `./demos/demo-3-late-event.sh` | Default policy drops/counts; configured lateness accepts and revises the open session | Watermarks turn completeness into an explicit policy (§§6.2–6.4) |
| Hot key | `./demos/demo-4-hot-key.sh` | One unsalted owner's real input queue saturates while siblings idle; salted local work is flat and output is unchanged | Per-subtask metrics reveal skew; two-stage aggregation fixes it (§§5.3, 9) |

The salting flag is `-Dlms.sessions.salted=true` on a Java entry point, or
`LMS_SESSIONS_SALTED=true` for Compose/Gradle submission (the JVM property takes precedence).
Set it before submitting a fresh job; it does not mutate an already-running DAG.
It changes only the session branch:

```text
click -> deterministic salt -> HASH(member,salt) -> local session fragments
      -> HASH(member) -> global fragment merge -> the same SessionRow
```

The local stage uses 16 stable salts. The global stage waits one additional session gap before
publishing, which gives every local salt time to close; the business `sessionEnd` remains the
ordinary last-event-plus-gap value. Because `ClickEvent` has no immutable event id, byte-identical
duplicate payloads deliberately choose the same salt. This preserves replay determinism while
normal time-varying traffic is spread across the local stage.

The stress fixture uses one hot **member**, the session branch's real key, rather than the
article's catalog-item example. Demo 4 feeds the same fixed clicks through the production session
topology in both modes and applies the same per-click work cost to both. It observes bounded-queue
backpressure and per-subtask counts, not a capacity estimate masquerading as a runtime signal.
Demo 3 exercises real source/task/watermark/state/timer loops locally. Its evidence is in
`lms-job/build/demo-3/report.txt`; process-recovery evidence is under
`lms-job/build/demo-evidence/`, and hot-key metrics, rows, and logs under `lms-job/build/demo-4/`.

### Late-data boundary

`SessionAggregator()` uses zero allowed lateness: an event behind the current watermark is counted
in `late-session-events` and dropped. `SessionAggregator(Duration)` may accept a behind-watermark
event while the current session is still retained and increments `accepted-late-session-events`.
The teaching engine does not retain and rewrite already-emitted historical sessions, and the
Iceberg sink is append-only rather than an upsert sink. The demo therefore proves open-window
revision versus drop; reopening closed output would require equality deletes or a keyed upsert
table and is intentionally not claimed.

---

## Known limitations

Mirrors §15 of the companion PDF. These are scope decisions, not oversights.

| Limitation | Why it is out of scope | The extension, if you want it |
|---|---|---|
| Master is a single point of failure | Leader election is Part 6 material; Flink has the same property without HA configured | etcd already holds the metadata — add a lease-based election and a standby master |
| No partition-safe execution-epoch fencing | A failure detector cannot distinguish a dead worker from an unreachable live writer; cancellation is best effort across a network partition | Persist execution epochs and enforce sink-side fencing before claiming exactly-once under partitions |
| No dynamic rescaling | Parallelism is fixed at submission. Key groups are implemented, so the hard part is done | Add a savepoint command, restore at a different parallelism, let `KeyGroupAssigner` redistribute |
| No savepoints | Phase 4 checkpoints are recovery points managed by the running job, without user-triggered retention or restore selection | `POST /jobs/{id}/savepoint` and a `--fromSavepoint` flag |
| No unaligned checkpoints | Phase 4 uses aligned barriers and buffers post-barrier records on blocked channels | Persist in-flight channel buffers for unaligned checkpoints |
| Iceberg sink supports unpartitioned tables only | Phase 6 keeps the file-to-append protocol legible and validates the destination spec | Add partition transforms and partition-aware `DataFile` construction |
| No terminal checkpoint for bounded transactional jobs | Periodic checkpoints cover the Kafka-based LMS job, but a bounded source can end after its last barrier | Add an end-of-input protocol in which all sources request and wait for a coordinator-owned final checkpoint before emitting terminal watermarks |
| No SQL or higher-level API | Framework DSLs are Parts 9B and 9C | A minimal SQL parser producing a `JobGraph` |
| No security, multi-tenancy or resource isolation | Orthogonal to every mechanism being taught | — |
| Task wire identities are operator/subtask scoped | Concurrent jobs that reuse operator ids are not isolated by the transport registry | Include job and execution identity in every channel and cancellation contract |

Additionally, Phase 4 recovery remains at-least-once for non-transactional operators such as the
console sink. Phase 6 provides exactly-once visible rows for completed checkpoint intervals in its
unpartitioned Iceberg sink for the tested process-crash/recovery paths, while orphaned objects still
require normal object-store maintenance. This is not a network-partition exactly-once guarantee.
The production LMS inputs are unbounded Kafka sources; bounded sources currently need an explicit
checkpoint before exhaustion or their final post-checkpoint interval remains an orphan.
`FileReplaySource` is bounded and deterministic. Job classes reach master and workers through a
startup classpath (or the shared Compose image) rather than being shipped with submission, so
every process needs the same classpath and changing the job means restarting them.

---

## Where this lands in the series

Part 9B takes these same mechanisms into Apache Storm, Apache Flink, Spark Structured Streaming
and Kafka Streams — four production answers to the problems this engine makes you solve
yourself. Reading their architecture docs after finishing Phase 7 is a noticeably different
experience from reading them before.
