# Distributed Stream Processing Engine

A teaching implementation of a distributed stream processing engine in Java 21: master and
worker processes, event-time watermarks, consistent checkpoints via barrier alignment, and
exactly-once output to Apache Iceberg.

It accompanies **Part 9A: Stream Processing Fundamentals** of the *Developing Intuition on
Building Blocks — Systems Design* series. The article explains how an engine like this works;
this repository is a working one, built phase by phase so that each mechanism is demonstrable
on its own before the next is added.

The design rationale, interfaces and algorithm sketches live in
[`docs/Part9A_Project_Companion.pdf`](docs/Part9A_Project_Companion.pdf).
[`CLAUDE.md`](CLAUDE.md) is the authoritative build instruction.

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
| 2 | Master and worker processes, gRPC transport, etcd | Not started |
| 3 | Event time, watermarks, session windows | Not started |
| 4 | Checkpointing, barrier alignment, recovery | Not started |
| 5 | The interval join | Not started |
| 6 | Transactional Iceberg sink | Not started |
| 7 | Status API and the four demos | Not started |

---

## Quickstart

Requires **JDK 21+** and **Docker**.

```bash
docker compose up -d          # Kafka (KRaft), topics created with 4 partitions
./demos/seed-clicks.sh        # publish the click fixture
./gradlew :lms-job:run        # run the job; Ctrl-C to stop
```

You should see 15 lines. The fixture holds 20 events and `BotFilter` drops five: three from
`bot-*` member ids, one with a blank member id, and one with no event time.

To watch offsets being committed and resumed:

```bash
./gradlew :lms-job:run        # run once, let it consume, Ctrl-C
./gradlew :lms-job:run        # run again: nothing, it resumed from the committed offset
./demos/seed-clicks.sh        # publish more
./gradlew :lms-job:run        # 15 more lines
```

Run the tests with `./gradlew test`. Tear everything down with `docker compose down -v`.

### A note on output ordering

Printed lines are not in event-time order, and per member they are not in publish order either.
That is not a bug and it is worth understanding. The sink runs at parallelism 2 behind a filter
at parallelism 4, so the edge between them is a **rebalance**: records round-robin across the
two sink subtasks, and two threads print independently. Ordering survives inside a partition
and across a forward edge; a redistributing exchange gives it up. Phase 3's session aggregator
is where that begins to matter, and where a `keyBy` replaces the rebalance.

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
engine-runtime/      → engine-api
                     Task run loop, output routing, state backends, serialization, transport.
engine-master/       → engine-api, engine-runtime
                     JobMaster, Scheduler, ExecutionGraph compiler, CheckpointCoordinator.
engine-worker/       → engine-api, engine-runtime
                     Worker bootstrap, task deployment, gRPC servers, heartbeat client.
engine-connectors/   → engine-api, engine-runtime
                     KafkaSource, IcebergSink, ConsoleSink, FileReplaySource.
lms-job/             → engine-api, engine-connectors
                     The LMS clickstream job, and its main().
```

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

Both were found by running the thing, and both are pinned by tests.

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

## Known limitations

Mirrors §15 of the companion PDF. These are scope decisions, not oversights.

| Limitation | Why it is out of scope | The extension, if you want it |
|---|---|---|
| Master is a single point of failure | Leader election is Part 6 material; Flink has the same property without HA configured | etcd already holds the metadata — add a lease-based election and a standby master |
| No dynamic rescaling | Parallelism is fixed at submission. Key groups are implemented, so the hard part is done | Add a savepoint command, restore at a different parallelism, let `KeyGroupAssigner` redistribute |
| No savepoints | Checkpoints exist; savepoints are checkpoints with a retention policy and stable operator ids, which the graph already assigns | `POST /jobs/{id}/savepoint` and a `--fromSavepoint` flag |
| Aligned checkpoints only | Alignment is the mechanism being taught; unaligned checkpointing would obscure it | Persist in-flight buffered records in the snapshot and skip channel blocking |
| No SQL or higher-level API | Framework DSLs are Parts 9B and 9C | A minimal SQL parser producing a `JobGraph` |
| No security, multi-tenancy or resource isolation | Orthogonal to every mechanism being taught | — |

Additionally, as of Phase 1: there is no distribution, no state, and no checkpointing. Offsets
are committed by Kafka on its own schedule, which is at-least-once and nothing stronger. Phase 4
replaces that with offsets held in the checkpoint.

---

## Where this lands in the series

Part 9B takes these same mechanisms into Apache Storm, Apache Flink, Spark Structured Streaming
and Kafka Streams — four production answers to the problems this engine makes you solve
yourself. Reading their architecture docs after finishing Phase 7 is a noticeably different
experience from reading them before.
