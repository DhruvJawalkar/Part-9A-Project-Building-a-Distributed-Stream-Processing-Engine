# CLAUDE.md — Distributed Stream Processing Engine

Build guide for an AI coding agent. Read this file completely before writing any code.

This project accompanies **Part 9A: Stream Processing Fundamentals** of the *Developing Intuition on Building Blocks — Systems Design* article series. The companion PDF (`Part9A_Project_Companion.pdf`, in `/docs`) contains the design rationale, interfaces and critical algorithm snippets. **This file is the authoritative build instruction; where it disagrees with the PDF, this file wins.**

---

## 1. What we are building, and the one rule that matters

A distributed stream processing engine — master process, N worker processes, real network transport, event-time windows, consistent checkpointing, exactly-once output to Apache Iceberg.

**The single most important rule: this is a teaching engine.** Its purpose is to make the mechanisms *legible*, not to be production-grade. When a choice arises between a clever optimisation and an obvious implementation that clearly shows the mechanism, **choose the obvious one**. A reader should be able to open `BarrierAligner.java` and see Chandy-Lamport happening.

Corollaries:
- Prefer explicit code over framework magic. No Spring, no dependency-injection container, no annotation processing. Plain constructors and `main()` methods.
- Name classes after the concepts in the article: `CheckpointCoordinator`, `BarrierAligner`, `WatermarkTracker`, `KeyGroupAssigner`, `IntervalJoinOperator`. Someone reading the article should recognise the file names.
- Comment the *why*, not the *what*. Especially where an implementation looks wrong but is deliberate (see §4).

---

## 2. Stack and conventions

| Concern | Choice |
|---|---|
| Language | Java 21 (sealed interfaces, records, pattern-matching `switch` are used throughout) |
| Build | Gradle (Kotlin DSL), multi-module |
| RPC | gRPC + protobuf |
| State backend | In-memory (Phase 1–3), RocksDB via `org.rocksdb:rocksdbjni` (Phase 4+) |
| Metadata store | etcd via `io.etcd:jetcd-core` |
| Source | Apache Kafka (`kafka-clients`) |
| Sink | Apache Iceberg (`iceberg-core`, `iceberg-parquet`), REST catalog |
| Object storage | MinIO (S3-compatible) for checkpoints and the Iceberg warehouse |
| Metrics | Micrometer → Prometheus |
| Testing | JUnit 5, AssertJ, Testcontainers (Kafka, etcd, MinIO) |
| Logging | SLF4J + Logback. Log at INFO for lifecycle events, DEBUG for per-record paths. Never log per record at INFO. |

Code style: standard Java conventions, 4-space indent, 110-column soft limit. Records for all data carriers. `Optional` for absent returns, never `null` in public APIs.

---

## 3. Module layout

Create these Gradle modules. The dependency direction is strict and must not be violated.

```
engine-api/          No runtime dependencies. StreamElement, Operator, KeyedOperator,
                     OperatorContext, ValueState, ListState, StateBackend, JobGraph builder,
                     ExchangeStrategy, KeyGroupAssigner.
engine-rpc/          No engine dependencies (added — see below).
                     The .proto wire contracts and the stubs generated from them.
engine-runtime/      depends on: engine-api, engine-rpc
                     Task run loop, WatermarkTracker, BarrierAligner, TimerService,
                     InMemoryStateBackend, RocksDbStateBackend, serialization, RecordTransport.
engine-master/       depends on: engine-api, engine-rpc, engine-runtime (graph types only)
                     JobMaster, Scheduler, ExecutionGraph compiler, CheckpointCoordinator,
                     TaskTracker, EtcdMetadataStore, StatusApi.
engine-worker/       depends on: engine-api, engine-rpc, engine-runtime
                     Worker bootstrap, task deployment, gRPC servers, heartbeat client.
engine-connectors/   depends on: engine-api, engine-runtime
                     KafkaSource, IcebergSink, ConsoleSink, FileReplaySource (for demos).
lms-job/             depends on: engine-api, engine-connectors (amended — see below).
                     The LMS clickstream job: BotFilter, SessionAggregator, ConversionJoin,
                     event records, and the job's main().
```

**`lms-job` must never see `engine-runtime`.** If the job needs something from `engine-runtime`, that is a signal the API is wrong — fix the API, do not add the dependency. This discipline is what keeps the user-facing surface honest.

> **Amendment — Phase 1, 2026-09-14.** The original rule here was `engine-api` **ONLY**. That
> made PDF §5.4's `KafkaSource.of("lms.catalog.clicks", ClickEvent.class)` unwritable, because
> `KafkaSource` lives in `engine-connectors` (§3 of this file puts it there). Given the choice
> between rewriting the article's snippet and relaxing the rule, the rule was relaxed:
> `lms-job` compiles against `engine-api` **and** `engine-connectors`, so the job reads exactly
> as the article prints it.
>
> The part of the discipline that carries the weight survives intact. `engine-connectors`
> declares `engine-runtime` as `implementation`, not `api`, so runtime internals never reach
> `lms-job`'s compile classpath. Gradle enforces it, not good intentions — if a job class
> cannot see a runtime type, that is the rule working.

> **Amendment — Phase 2, 2026-09-16.** Added a seventh module, `engine-rpc`, holding the
> `.proto` contracts and the stubs generated from them. The control plane has two ends:
> `engine-master` serves `MasterService` and calls `WorkerService`; `engine-worker` does the
> reverse. Both need the same generated types and neither should depend on the other. The
> contracts had been put in `engine-runtime` purely because it was the only existing module both
> could see, which made the runtime the accidental owner of definitions it has no stake in.
>
> The six-module count is not itself load-bearing; the dependency **direction** is. A module
> holding only wire contracts, depending on no engine module, points inward from both sides and
> leaves that direction intact.

---

## 4. Deviations from the companion PDF

Apply these. The PDF was written first; these supersede it.

### 4.1 `MAX_PARALLELISM` → `NUM_KEY_GROUPS`

The PDF's `KeyGroupAssigner` uses the constant name `MAX_PARALLELISM`. **Rename it to `NUM_KEY_GROUPS`** and expose the job-level config knob as `maxParallelism`. The PDF's name conflates two different things and confuses readers.

```java
public final class KeyGroupAssigner {
    /** Number of key groups. NOT the number of running tasks.
     *  Fixed at job creation; changing it invalidates all existing checkpointed state,
     *  because every key would hash into a different group. */
    public static final int NUM_KEY_GROUPS = 128;

    public static int keyGroupFor(Object key) {
        return Math.abs(murmurHash(key.hashCode())) % NUM_KEY_GROUPS;
    }

    /** Key groups are assigned to subtasks in contiguous ranges, so rescaling moves
     *  whole groups rather than individual keys. */
    public static int subtaskFor(Object key, int parallelism) {
        return keyGroupFor(key) * parallelism / NUM_KEY_GROUPS;
    }
}
```

Add a class-level Javadoc explaining the two-hop mapping: `key → key group (fixed forever) → subtask (changes on rescale)`. This is the concept the indirection exists for, and the code is where a reader will look for it.

Everywhere else the PDF says "MAX_PARALLELISM", read "NUM_KEY_GROUPS".

### 4.2 A slot is one vertical pipeline slice

PDF §8.1 contradicts itself within a sentence: "round-robin **subtasks** across registered
workers, subject to one rule — all subtasks of a chained group land **together**." Both cannot
hold. Its code snippet assigns whole chain groups; its prose says "a slot in this project is
one vertical pipeline slice, mirroring Flink rather than Storm."

**Follow the prose.** The unit of scheduling is subtask *i* of every operator in a chain group,
together — one vertical slice. Round-robin distributes slices, and it does *not* apply within a
slice: the chained operators of one slice are fused into a single thread and are not separately
schedulable.

Assigning whole chain groups instead would cap a job's spread at its number of chain groups,
however parallel its operators were. The Phase 1 LMS job compiles to two chain groups
(`clicks→drop-bots` fused, then `console`), so on a three-worker cluster one worker would sit
idle and Phase 2's acceptance criterion "a submitted job distributes across all three" would
fail on its own terms. As vertical slices the same job compiles to six tasks across all three.

### 4.3 Everything else in the PDF stands

Interfaces in PDF §5, gRPC contracts in §6, and the phase structure in §7–13 are all authoritative. Follow them closely — the article cross-references specific names.

---

## 5. Phase gating — read this before starting any phase

The build is **strictly sequential**. Each phase must reach its acceptance criteria and be committed before the next begins.

**Do not skip ahead.** Do not implement checkpointing in Phase 2 because it seems convenient. The phases are ordered so each one is independently demonstrable, which is the entire pedagogical value of the project.

At the start of each phase, state which phase you are on and what its acceptance criteria are. At the end, verify each criterion explicitly before proposing a commit.

Commit format: `phase(N): <what was built>` — e.g. `phase(4): checkpoint coordinator and barrier alignment`.

---

## 6. The phases

### Phase 1 — Single-process engine
**Goal:** one JVM, Kafka → filter → console. No distribution, no state, no checkpoints.

Build:
1. `engine-api` in full: `StreamElement` sealed hierarchy, `Operator`, `KeyedOperator`, `OperatorContext`, state interfaces, `JobGraph` fluent builder, `ExchangeStrategy`, `KeyGroupAssigner`.
2. `JobGraph` validation: DAG has no cycles, every operator reachable from a source, parallelism ≥ 1. Fail fast with a clear message.
3. `Task` as a `Runnable` with an inbound `BlockingQueue<StreamElement>`, one operator instance, and a `Collector` pushing to downstream queues. **Write the run loop with the `switch` over `StreamRecord` / `Watermark` / `CheckpointBarrier` already in place** — leave the latter two branches as no-op stubs with a `// Phase 3` / `// Phase 4` comment. This avoids a rewrite later.
4. `KafkaSource` around a plain `KafkaConsumer`; offsets tracked in a field for now.
5. `lms-job`: `ClickEvent`, `BorrowEvent` records, `BotFilter`, and a `main()` that builds and runs the job.
6. `ConsoleSink`.

**Acceptance:**
- [ ] `./gradlew run` in `lms-job` consumes from `lms.catalog.clicks` and prints filtered events.
- [ ] Killing and restarting reprocesses from the committed consumer offset.
- [ ] `engine-api` has zero dependencies in its `build.gradle.kts`.
- [ ] Unit test: `JobGraph` builder rejects a cyclic graph and a zero-parallelism operator.

---

### Phase 2 — Master and worker processes
**Goal:** one master, N workers, records genuinely crossing process boundaries.

Build:
1. Protobuf definitions for `MasterService`, `WorkerService`, `DataTransportService` exactly as PDF §6.
2. `ExecutionGraph` compiler (PDF §8.1): expand operators to subtasks, determine exchange strategy per edge, build chain groups (adjacent operators with equal parallelism *and* a FORWARD exchange), assign chain groups round-robin to workers.
3. `EtcdMetadataStore` with the key layout from PDF §8.3. **Persist every job state transition to etcd before acting on it.**
4. Worker registration with a TTL lease; master watches the `/workers/` prefix rather than polling.
5. Heartbeat: bidirectional stream, 1s interval, worker declared dead after 3 missed beats.
6. `RecordTransport`: serialization, exchange-strategy routing, gRPC streaming between workers. Batch multiple elements per `DataBuffer`; flush when full **or** when `bufferTimeoutMs` (default 100ms) expires — expose this as config, it is the article's latency/throughput dial.
7. Credit-based flow control: receiver grants credit, sender ships within it.
8. Job state machine: `CREATED → RUNNING → FINISHED`, plus `FAILING → FAILED`. **Phase 2's failure response is to fail the job** — there is nothing to recover to yet. Do not add restart logic here.

**Acceptance:**
- [ ] Three worker processes start independently, register, and appear in etcd.
- [ ] A submitted job distributes across all three; verify via logs that records cross processes.
- [ ] `kill -9` on a worker is detected within ~3s; job transitions to FAILED with the cause recorded in etcd.
- [ ] Restarting the master recovers job graph and assignments from etcd.
- [ ] Chained operators (filter + map at equal parallelism) run in one thread with no serialization — assert this in a test.

---

### Phase 3 — Event time, watermarks, session windows
**Goal:** the engine starts caring about when events happened.

Build:
1. `BoundedOutOfOrdernessGenerator` (PDF §9.1) including **idleness detection** — this is not optional, it is a demonstrated behaviour.
2. `WatermarkTracker`: per-channel watermarks, minimum across non-idle channels, only advance on progress (PDF §9.2). Fill in the Phase 1 stub.
3. `TimerService`: event-time timers registered per key, fired in timestamp order when the watermark advances, with the key context restored before the callback.
4. `InMemoryStateBackend` implementing `ValueState` / `ListState`, scoped by `setCurrentKey`.
5. Keyed exchange: `HASH` routing via `KeyGroupAssigner.subtaskFor`.
6. `SessionAggregator` (PDF §9.3) — **incremental accumulation, not a buffered element list.** Include the superseded-timer check; it is subtle and easy to omit.

**Acceptance:**
- [ ] Replaying a fixed file of out-of-order events produces byte-identical session output on every run.
- [ ] With idleness detection disabled, a silent partition stalls all output. With it enabled, it does not. **Write this as an automated test** — it is the article's §6.4 bug, and the test is the proof.
- [ ] Session extension works: an event arriving before a session's gap expires pushes the window end out and does not emit early.
- [ ] Per-member state is cleared after the session fires (assert state size returns to zero).

---

### Phase 4 — Checkpointing, barrier alignment, recovery
**Goal:** the centrepiece. Consistent snapshots without stopping the stream.

Build:
1. `CheckpointCoordinator` on the master (PDF §10.1): interval trigger, barrier injection at sources only, per-task ack collection, completion when **every** task has acknowledged, then `notifyCheckpointComplete` to sinks. Include a timeout that aborts an incomplete checkpoint.
2. `BarrierAligner` (PDF §10.2). Single-input tasks skip alignment entirely. Multi-input tasks block each channel as its barrier arrives, keep consuming unblocked channels, snapshot once all barriers are in, **forward the barrier before acknowledging**, then drain buffered records.
3. Record `alignmentMillis` and `stateSizeBytes` per checkpoint and report them on the ack. This is the article's §10.5 cost made observable.
4. `RocksDbStateBackend`: one column family per state name, snapshot to MinIO, restore from a state handle.
5. Source snapshots: Kafka offsets into the checkpoint, **not** committed to Kafka.
6. Recovery (PDF §10.3): cancel every task of the job, read the latest checkpoint pointer from etcd, redeploy all tasks with their state handles, `restore()`, sources `seek()`, resume. **Restart the whole job, not just the failed task.** Add a comment saying why — a reader will assume it is a bug otherwise.
7. Restart strategy: fixed-delay, N attempts, then FAILED.

**Acceptance:**
- [ ] Checkpoints complete on a 10s interval (configurable); alignment time visible per task.
- [ ] **The core test:** run a fixed replay, `kill -9` a worker holding session state mid-window, let the job recover and finish. Output must be byte-identical to a clean run. Automate this.
- [ ] Checkpoint duration, state size and alignment time appear in the heartbeat.
- [ ] Restart-from-checkpoint uses the checkpoint's Kafka offsets, not Kafka's committed offsets.

---

### Phase 5 — The interval join
**Goal:** match RESULT_CLICK → BORROW within 30 minutes, buffering both sides.

Build:
1. `IntervalJoinOperator` (PDF §11.1): both sides buffered keyed by `(memberId, catalogItemId)`, each entry timestamped, bidirectional matching.
2. Watermark-driven eviction (PDF §11.2) — a click evictable once `ts + UPPER_BOUND < watermark`, a borrow once `ts < watermark − UPPER_BOUND`.
3. Buffer-size metrics per side.

**Acceptance:**
- [ ] Clicks and borrows arriving in either order produce exactly one conversion row per match.
- [ ] Buffer size rises then plateaus under sustained load — assert it does not grow monotonically.
- [ ] Deliberately lagging one input inflates the *other* side's buffer. Demonstrate this; it is the slower-stream effect from article §8.3.

---

### Phase 6 — Transactional Iceberg sink
**Goal:** end-to-end exactly-once output.

Build:
1. `IcebergSink` implementing the 2PC mapping in PDF §12.1: open writer per checkpoint interval, write uncommitted Parquet files, on `preCommit` close the writer and store file paths in operator state, on `notifyCheckpointComplete` do one atomic `AppendFiles.commit()`.
2. **`commit()` must be idempotent** — a crash between commit and state-clearing replays it. Guard by checking whether the checkpoint's files are already in the current snapshot.
3. Iceberg table DDL for `lms.analytics.browse_sessions` and `lms.analytics.click_conversions`.

**Acceptance:**
- [ ] Querying the table mid-interval returns nothing for that interval; the whole interval appears at once on checkpoint completion.
- [ ] Killing a worker mid-interval leaves orphaned Parquet files referenced by no snapshot, and each session row appears in the table exactly once.
- [ ] Shortening the checkpoint interval measurably improves freshness and measurably increases small-file count. Record both numbers in the README.

---

### Phase 7 — Status API and the four demos
**Goal:** make the article's claims falsifiable.

Build:
1. REST endpoints exactly as PDF §13.1, plus a Prometheus scrape endpoint on master and workers.
2. `docker-compose.yml` with all services from PDF §14.
3. Grafana dashboard with four panels: source lag, checkpoint duration + alignment time, per-subtask records-in (to show skew), state size per subtask.
4. A `demos/` directory with one runnable script per demo, each printing what to watch for before it runs.

The four demos (PDF §13.2): worker dies mid-window; master dies; late event; hot key. Each script must be reproducible from the fixture file, not dependent on live traffic.

5. `SaltedSessionAggregator` — the two-phase local-then-global fix for Demo 4, enabled by a config flag so the demo can run both ways.

**Acceptance:**
- [ ] `docker compose up` brings up the full stack and the seed job publishes the fixture.
- [ ] All four demo scripts run end to end and produce the expected observable outcome.
- [ ] Demo 4 shows one subtask backpressured with siblings idle, then flat distribution after enabling salting.
- [ ] README documents each demo: what to run, what to watch, what it proves, and which article section it corresponds to.

---

## 7. Testing expectations

- Unit tests for every operator in `lms-job` and every algorithm class in `engine-runtime` (`WatermarkTracker`, `BarrierAligner`, `KeyGroupAssigner`, `TimerService`).
- Testcontainers integration tests from Phase 2 onward for Kafka, etcd and MinIO.
- **The two tests that matter most**, both automated:
  1. Phase 3's idle-partition stall test.
  2. Phase 4's kill-a-worker-mid-window determinism test.

  These two prove the project's central claims. Treat a regression in either as a build failure, not a flaky test to retry.
- Determinism tests must use `FileReplaySource` with a fixed fixture, never live Kafka traffic.

---

## 8. Documentation to produce alongside the code

- `README.md` — what this is, its relationship to the article, quickstart, the four demos, and an explicit "known limitations" section mirroring PDF §15. Link the article.
- `docs/ARCHITECTURE.md` — the process topology diagram and the component table, kept current as phases land.
- Javadoc on every `engine-api` type. These are the reader-facing abstractions; treat their docs as part of the deliverable.

---

## 9. When you are unsure

Ask rather than guess, specifically when:
- A design choice would deviate from the companion PDF's interfaces.
- An optimisation would make a mechanism less legible in the source.
- A phase's acceptance criterion seems impossible as written.

Do not silently "improve" the design. The design is load-bearing for an article that explains it.
