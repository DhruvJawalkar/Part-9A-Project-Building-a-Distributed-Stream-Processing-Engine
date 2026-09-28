# Distributed Stream Processing Engine

A teaching implementation of a distributed stream processing engine in Java 21. It currently
runs master and worker processes, routes records over gRPC, and processes keyed streams in event
time; checkpointing and exactly-once Iceberg output are the next planned phases.

It accompanies **Part 9A: Stream Processing Fundamentals** of the *Developing Intuition on
Building Blocks — Systems Design* series. The article explains how an engine like this works;
this repository is a working one, built phase by phase so that each mechanism is demonstrable
on its own before the next is added.

The design rationale, interfaces and algorithm sketches live in
[`docs/Part9A_Project_Companion.pdf`](docs/Part9A_Project_Companion.pdf).
[`CLAUDE.md`](CLAUDE.md) is the authoritative build instruction.
[`docs/DESIGN.md`](docs/DESIGN.md) is the living implementation design.
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
| 4 | Checkpointing, barrier alignment, recovery | Planned |
| 5 | The interval join | Planned |
| 6 | Transactional Iceberg sink | Planned |
| 7 | Status API and the four demos | Planned |

---

## Quickstart

Requires **JDK 21+** and **Docker**.

```bash
docker compose up -d          # Kafka (KRaft), topics created with 4 partitions
./demos/seed-clicks.sh        # publish the click fixture
./gradlew :lms-job:run        # run the job; Ctrl-C to stop
```

The job now prints `SessionRow` values, rather than one line per click. A row appears when the
event-time watermark reaches the session end (15 minutes after the latest event for that member).
That delay is deliberate: it is the proof that the job is using when an event happened, not when
the process happened to receive it.

To watch offsets being committed and resumed:

```bash
./gradlew :lms-job:run        # run once, let it consume, Ctrl-C
./gradlew :lms-job:run        # run again: Kafka resumes from its committed offset
./demos/seed-clicks.sh        # publish more
./gradlew :lms-job:run        # observe rows for the new event-time sessions
```

Run the full test suite with `./gradlew test`. The focused Phase 3 proof is:

```bash
./gradlew :engine-runtime:test :lms-job:test :engine-connectors:test
```

It covers bounded out-of-orderness, the silent-partition/idleness regression, keyed routing and
state, timer key restoration, session extension, stale-timer suppression, state clearing, and
deterministic file replay. Tear everything down with `docker compose down -v`.

### A note on output ordering

Printed session rows are not globally ordered, and that is not a bug. The `by-member` hash edge
keeps each member on one session subtask, but different member keys run independently and the
two sink subtasks print independently. Ordering survives within one keyed partition; a global
ordering would require an explicit downstream coordination point and is not part of this engine.

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
                     KeyGroupAssigner.
engine-rpc/          No engine dependencies. The .proto wire contracts and generated stubs.
engine-metadata/     No engine dependencies. MetadataStore, etcd and in-memory implementations.
engine-runtime/      → engine-api, engine-rpc
                     Task run loop, event time, keyed state and timers, input gates, transport,
                     serialization.
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

## Known limitations

Mirrors §15 of the companion PDF. These are scope decisions, not oversights.

| Limitation | Why it is out of scope | The extension, if you want it |
|---|---|---|
| Master is a single point of failure | Leader election is Part 6 material; Flink has the same property without HA configured | etcd already holds the metadata — add a lease-based election and a standby master |
| No dynamic rescaling | Parallelism is fixed at submission. Key groups are implemented, so the hard part is done | Add a savepoint command, restore at a different parallelism, let `KeyGroupAssigner` redistribute |
| No savepoints | Checkpointing itself is planned for Phase 4; savepoints come after it with a retention policy and stable operator ids | `POST /jobs/{id}/savepoint` and a `--fromSavepoint` flag |
| No checkpointing yet | Phase 4 introduces aligned checkpoints because the mechanism is the lesson | Later persist in-flight buffered records for unaligned checkpoints |
| No SQL or higher-level API | Framework DSLs are Parts 9B and 9C | A minimal SQL parser producing a `JobGraph` |
| No security, multi-tenancy or resource isolation | Orthogonal to every mechanism being taught | — |

Additionally, as of Phase 3: state is heap-backed and is not yet included in a coordinated job
checkpoint. A worker death still fails the job outright; there is no consistent whole-job point
to rewind to until planned Phase 4. Kafka offsets remain broker-managed, so the end-to-end
delivery guarantee is at-least-once rather than exactly-once. `FileReplaySource` is bounded and
deterministic, but the Phase 7 runnable demos have not been added. Job classes reach master and
workers through a `JOB_CLASSPATH` set at startup rather than being shipped with submission, so
every process needs the same classpath and changing the job means restarting them.

---

## Where this lands in the series

Part 9B takes these same mechanisms into Apache Storm, Apache Flink, Spark Structured Streaming
and Kafka Streams — four production answers to the problems this engine makes you solve
yourself. Reading their architecture docs after finishing Phase 7 is a noticeably different
experience from reading them before.
