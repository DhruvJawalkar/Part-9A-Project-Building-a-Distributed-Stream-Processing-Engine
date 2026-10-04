# Bug log

A running record of bugs found while building the engine, kept so they can be reviewed in one
pass rather than reconstructed from commit messages later.

Every entry here was found by **running the thing**, not by a test failing. That is the point of
keeping the log: the tests came second in each case, written to pin a bug that had already
happened. Which bugs a design lets you make is worth knowing.

**Status key:** `FIXED` — fixed and pinned by a regression test or repeatable runtime check · `OPEN` — known, not yet addressed ·
`ACCEPTED` — understood, deliberately not fixed, with a reason.

---

## Index

| # | Phase | Bug | Status | Silent? |
|---|---|---|---|---|
| [1](#1-every-subtask-needs-its-own-operator-instance) | 1 | Every subtask shared one operator instance | FIXED | Partly |
| [2](#2-a-forward-edge-broadcast-to-every-channel) | 1 | A forward edge broadcast to every channel | FIXED | Yes |
| [3](#3-credit-deadlock-at-stream-open) | 2 | Credit deadlock at stream open | FIXED | Yes |
| [4](#4-grpc-context-cancellation-killed-the-record-streams) | 2 | gRPC `Context` cancellation killed the record streams | FIXED | Nearly |
| [5](#5-operatorchain-zeroed-event-time) | 2 | `OperatorChain` zeroed event time | FIXED | Yes |
| [6](#6-the-client-deployed-the-job-without-telling-the-master) | 2 | The client deployed the job without telling the master | FIXED | Yes |
| [7](#7-a-silent-partition-stalled-event-time) | 3 | A silent partition stalled event time | FIXED | Yes |
| [8](#8-an-idle-channel-could-make-max-look-safe) | 3 | An idle channel could make MAX look safe | FIXED | Yes |
| [9](#9-keyed-state-crossed-an-operator-chain-boundary) | 3 | Keyed state crossed an operator-chain boundary | FIXED | Yes |
| [10](#10-session-windows-merged-events-across-a-real-gap) | 3 | Session windows merged events across a real gap | FIXED | Yes |
| [11](#11-windows-line-endings-blocked-the-git-bash-cluster-script) | 4 | Windows line endings blocked the Git Bash cluster script | FIXED | No |
| [12](#12-kafka-interrupt-during-worker-cancellation-looked-like-task-failure) | 4 | Kafka interrupt during worker cancellation looked like task failure | FIXED | No |
| [13](#13-scheduled-flush-held-a-monitor-during-credit-wait) | 4 | Scheduled flush held a monitor during credit wait | FIXED | No |
| [14](#14-user-code-had-two-class-identities-in-the-image) | 7 | User code had two class identities in the image | FIXED | No |
| [15](#15-restoration-used-the-wrong-classloader) | 7 | Restoration used the wrong classloader | FIXED | No |
| [16](#16-a-synchronous-checkpoint-ack-deadlocked-sink-completion) | 7 | A synchronous checkpoint ACK deadlocked sink completion | FIXED | Nearly |
| [17](#17-concurrent-sink-commits-locked-the-demo-catalog) | 7 | Concurrent sink commits locked the demo catalog | FIXED | No |
| [18](#18-registration-preceded-heartbeats-by-too-long) | 7 | Registration preceded heartbeats by too long | FIXED | Nearly |
| [19](#19-the-catalog-lost-table-pointers-on-recreation) | 7 | The catalog lost table pointers on recreation | FIXED | Yes |
| [20](#20-the-short-fixture-never-closed-its-sessions) | 7 | The short fixture never closed its sessions | FIXED | Yes |
| [21](#21-a-retired-heartbeat-stream-overwrote-its-replacement) | 7 | A retired heartbeat stream overwrote its replacement | FIXED | Yes |
| [22](#22-a-cold-master-never-scheduled-its-recovered-job) | 7 | A cold master never scheduled its recovered job | FIXED | Yes |

### The pattern worth noticing

**Eight of the first ten produced no error at all.** No exception, no failed health check, no red log line
— just a job that was up, reported as `RUNNING`, and quietly not doing its work. The Phase 3
entries are the classic forms: a window that never closes, a window that closes too early, or
state attached to the wrong operator. All can look like an ordinary data-quality issue.

That is the characteristic failure mode of a distributed stream processor, and it is the same
shape as Demo 4 in Phase 7: *it fails while looking entirely healthy*. Worth remembering when
writing the article — these are not embarrassments to hide, they are the argument for why the
mechanisms exist.

The Phase 4 run issues below were operationally visible: one prevented cluster startup from the
shell, and two could stall or confuse task cancellation during recovery.

---

## 1. Every subtask needs its own operator instance

**Phase 1 · FIXED**

A job graph holds one operator object — the instance the author constructed in `main()`. Running
that operator four ways handed the *same object* to four threads.

With a `KafkaSource` the failure is loud: `KafkaConsumer` checks for concurrent access and three
of the four subtasks died on first use with `ConcurrentModificationException`. With a plain field
it would have been silent, and would have looked like records going missing.

**Why a distributed engine never sees it.** Shipping a task to a worker deserializes it there, so
every subtask gets a private copy for free. Only a single-JVM executor can make this mistake —
which is exactly why the local executor has to imitate the distributed path rather than take the
shortcut.

- **Fix:** `TaskInstances.copyOf` — `engine-runtime/.../runtime/TaskInstances.java:48`. Copies by
  serializing and deserializing, which is what deployment does anyway.
- **Test:** `LocalJobExecutorTest` — *"each subtask gets its own operator instance"* (:105).
  Asserts four distinct `System.identityHashCode` values.
- **Bonus:** the same mechanism now fails loudly at startup for an operator that is not
  serializable, rather than at deployment in Phase 2.

---

## 2. A forward edge broadcast to every channel

**Phase 1 · FIXED**

`broadcast` sent control elements to every downstream queue regardless of exchange strategy. Under
a `FORWARD` exchange, subtask *i* feeds only subtask *i*, so this sent watermarks down channels
that carry no records from this task.

**Why it matters more later than it did then.** A spurious watermark is a claim about a stream the
task does not produce. A spurious **checkpoint barrier** in Phase 4 would be worse: it arrives on
a channel that will never deliver the records it is supposed to be separating, so alignment would
wait forever for a barrier that cannot come. The bug was harmless when introduced and would have
been extremely hard to diagnose two phases later.

- **Fix:** `ResultPartitionWriter.broadcast` —
  `engine-runtime/.../transport/ResultPartitionWriter.java:75`.
- **Test:** `LocalJobExecutorTest` — *"a narrower sink still receives every record, via
  rebalance"* (:87), plus `InputGateTest`'s ordering checks.

---

## 3. Credit deadlock at stream open

**Phase 2 · FIXED · silent**

A sender starts at **zero** credit and waits for permission before shipping anything. The receiver
granted credit only *after* receiving a buffer. So the sender was waiting for credit that would
only be sent in response to a buffer it was not allowed to send.

Both sides idle. Every process healthy. Job reported `RUNNING`. Not one record anywhere, and
nothing in any log to say why.

**The rule:** the opening grant must be sent when the stream *opens*, before any data. The proto
comment said exactly this; the implementation did the opposite.

- **Fix:** `DataTransportService.exchangeRecords` —
  `engine-runtime/.../transport/DataTransportService.java:67`. The grant is now the first thing
  the handler does, before returning the request observer.
- **Test:** `CreditFlowControlTest` — *"a sender ships records without waiting to be asked twice"*
  (:59). Runs over a real gRPC socket; would **hang** rather than fail if this regressed, hence
  the `@Timeout`.
- **Also pinned:** *"keeps sending well past the initial credit allowance"* — catches the
  opposite mistake, credit granted once and never renewed.

---

## 4. gRPC `Context` cancellation killed the record streams

**Phase 2 · FIXED · nearly silent**

The worker opens its outgoing record streams while handling the master's `DeployTask` call. A gRPC
client call started inside a server handler inherits that handler's `Context` by default — and a
server `Context` is cancelled the instant the handler returns.

So every record stream was destroyed milliseconds after the deployment that created it. The only
evidence was a single line per stream:

```
CANCELLED: io.grpc.Context was cancelled without error
```

which explains nothing on its own, and which is easy to read as ordinary shutdown noise.

**The rule:** a connection that must outlive the call that created it is opened under
`Context.ROOT`, which has no deadline and no cancellation.

- **Fix:** `DataTransportClient.openDetached` —
  `engine-runtime/.../transport/DataTransportClient.java:101`, using attach/detach around the stub
  call.
- **Test:** covered by `CreditFlowControlTest` end to end — the streams there are opened the same
  way and the test would time out if they died.
- **Watch for:** the same trap applies to any long-lived client call the worker or master opens
  from inside a handler. Phase 4's checkpoint paths are the next candidates.

---

## 5. `OperatorChain` zeroed event time

**Phase 2 · FIXED · silent**

`Collector.collect(value)` deliberately takes no timestamp — an operator that transforms a record
should not have to restate when the underlying event happened. The chain is what carries it, and
`OperatorChain.processElement` never seeded `currentTimestamp` from the incoming record.

Every operator behind the first one in a chain therefore saw event time **0** — the epoch.

Nothing failed. Nothing logged. In Phase 2 nothing even depended on it, because no operator reads
event time yet. In Phase 3 every session window in the job would have been silently wrong, and the
cause would have been three phases and several thousand lines away from the symptom.

**Caught by a test written for a different reason** — the only bug in this log not found by
running the cluster. Worth noting: the test was checking that event time survives a chain, which
seemed obvious enough to be barely worth asserting.

- **Fix:** `OperatorChain.processElement` — `engine-runtime/.../runtime/OperatorChain.java:78`.
- **Test:** `OperatorChainTest` — *"an intermediate operator passes event time to the next one"*
  (:73).
- **Related, not yet exercised:** `ChainedSourceOutput` carries its own `currentTimestamp` for the
  source-chaining path. It is set correctly, but no test covers it until Phase 3 gives a chained
  source something time-dependent to do. **See review note R1 below.**

---

## 6. The client deployed the job without telling the master

**Phase 2 · FIXED · silent**

`JobClient` originally compiled the graph and deployed the tasks itself, talking straight to the
workers. It worked, and it was fewer moving parts.

It was wrong in a way that only appeared when a worker died: the master had never been told the
job existed, so when its failure detector fired — correctly, in 2.85s — there was nothing in
`executionGraphs` to fail. The job carried on half-running with nobody supervising it, and the
`FAILED` state that should have been recorded in etcd never was.

**The rule, and it is the argument for a control plane in one sentence:** whoever is responsible
for reacting to failure has to be the one that knows what is running.

- **Fix:** `MasterService.submitJob` — `engine-master/.../master/MasterService.java:51`, plus a
  `SubmitJob` RPC in `master.proto`. `JobClient` now serializes and hands over, nothing more.
- **Test:** `JobMasterTest` — *"a dead worker fails the job, with the cause in the store"* (:87).
- **Consequence to remember:** the master now deserializes the submitted graph, so it needs the
  job's classes on its classpath exactly as every worker does. That is why `run-cluster.sh` passes
  `JOB_CLASSPATH` to the master too.

---

## 7. A silent partition stalled event time

**Phase 3 · FIXED · silent**

One source subtask continued to produce records while another stayed connected but produced
nothing. The downstream task took the minimum watermark across both channels, so the silent
channel held the task clock at `Long.MIN_VALUE`. The job was healthy and consumed records, but no
event-time timer or session window could ever fire.

**The rule:** no input may be excluded merely because no message has arrived; on a distributed
channel that absence is ambiguous. A source must explicitly report an idleness transition, and a
downstream task must take the minimum only across active channels.

- **Fix:** `BoundedOutOfOrdernessGenerator`, `SourceTask`, and `WatermarkTracker` —
  `engine-runtime/.../runtime/`. The source broadcasts `Watermark.IDLE` after its configured
  silence timeout, and `Watermark.ACTIVE` before the first resumed record.
- **Test:** `LocalJobExecutorTest` — *"a silent source channel no longer stalls event-time
  progress after idleness"* (:160). It first proves that no idleness stalls progress, then proves
  that the same input progresses with a 10 ms idle timeout.

---

## 8. An idle channel could make MAX look safe

**Phase 3 · FIXED · silent**

After introducing idleness, treating `Watermark.MAX` as an ordinary per-channel watermark had a
new failure mode: an idle channel was excluded from the minimum, so a different channel's EOF
could advance the task to MAX. That fires all remaining timers and closes state even though a
non-ended channel may still resume and deliver records.

**The rule:** idleness is a liveness hint, not an end-of-input declaration. MAX is safe only when
every physical input channel has announced EOF, regardless of its idle status.

- **Fix:** `OperatorTask.handleWatermark` counts EOF markers per channel and calls
  `WatermarkTracker.endOfAllInputs()` only after all have arrived. The ordinary tracker path
  never treats an idle channel as permission to emit MAX.
- **Coverage:** `OperatorTaskEventTimeTest` exercises timer flushing on MAX, while the task loop's
  per-channel EOF bookkeeping makes the all-inputs condition explicit. A dedicated multi-input
  EOF regression should remain part of the Phase 4 barrier-alignment coverage.

---

## 9. Keyed state crossed an operator-chain boundary

**Phase 3 · FIXED · silent**

The Phase 2 chain builder was allowed to fuse any equal-parallelism forward edge. A keyed
operator now owns a current key, state backend and timer service; fusing an ordinary neighbour
into that task would hand it the same context and state namespace. The records would still flow,
but state names from two operator instances could silently address the same keyed store.

**The rule:** a keyed task is an isolation boundary until the runtime namespaces state per
operator inside a chain. Chaining is an optimisation; state scope is correctness.

- **Fix:** `ChainBuilder.chainableSuccessorOf` rejects a chain whose upstream or successor is
  keyed, even if the edge is otherwise forward and parallelism matches.
- **Test:** `ChainBuilderTest` — *"condition 1: a hash exchange breaks the chain"* (:46) proves
  the LMS-shaped keyed boundary is a separate chain group.

---

## 10. Session windows merged events across a real gap

**Phase 3 · FIXED · silent**

`SessionAggregator` accumulated every arrival for a member into its current state. If an event
was later than the current session end by more than the 15-minute gap, it was nevertheless merged
into that old session if the watermark had not yet advanced. The output looked plausible, but one
member's distinct browsing visits became a single session.

**The rule:** a session is defined in event time, not by the moment its timer happens to run. An
event beyond the current end closes and resets the prior session; an event too old to merge with
the current open session is counted/dropped. There is no allowed-lateness reopening policy yet.

- **Fix:** `SessionAggregator` compares each event timestamp to the stored end before updating
  its accumulator, emits/resets when it starts a new session, and drops unmergeably late input.
- **Test:** `SessionAggregatorTest` covers extension, stale-timer suppression, state clearing,
  and the gap/late-event rules.

---

## 11. Windows line endings blocked the Git Bash cluster script

**Phase 4 · FIXED · visible**

The cluster launcher was checked out with Windows CRLF line endings. Git Bash treated the carriage
return at the end of the shebang as part of the interpreter path, so invoking the script failed
before it could start the master or workers. The source looked normal in an editor, making the
failure easy to misattribute to Docker or the Java launch command.

**The rule:** shell entry points that are launched from Git Bash need Unix line endings in the
working tree. The tracked script now has LF endings; the fix was verified by starting the cluster
script and observing the worker processes register.

- **Fix:** normalize `demos/run-cluster.sh` to LF in the repository.
- **Verification:** the Phase 4 three-worker recovery run started through this script.

---

## 12. Kafka interrupt during worker cancellation looked like task failure

**Phase 4 · FIXED · visible**

During whole-job recovery, the master intentionally cancels every task. Interrupting a worker's
Kafka poll can surface as Kafka's `InterruptException` rather than a plain `InterruptedException`.
If the source reports that connector exception after cancellation, the worker sends a task-failure
report for work the master itself just stopped. That can race with recovery and make normal
cancellation look like a new failure.

**The rule:** after cancellation fences a source task, connector exceptions from its active poll
are part of shutdown. Exceptions while the task is still running remain genuine failures.

- **Fix:** `SourceTask.run` checks its `running` flag in the general exception path and reports
  connector failures only while the task is still active.
- **Test:** `SourceTaskCheckpointTest.connectorWakeupDuringCancellationIsNotReportedAsATaskFailure`
  interrupts a connector poll after cancellation and verifies that no failure is reported.

## 13. Scheduled flush held a monitor during credit wait

**Phase 4 · FIXED · visible**

`RemoteSubpartition` schedules buffer flushes on a timer thread. A flush held the subpartition
monitor while waiting up to 120 seconds for downstream credit. During recovery, task cancellation
then tried to close the same output and could not publish closure or finish teardown until that
credit wait returned. The old task thread stayed alive, so the master could not safely redeploy its
replacement.

**The rule:** a close or cancellation path must be able to interrupt a producer waiting for credit,
make closure visible, and release the waiter before waiting for the task thread to stop. A delayed
flush must recheck the closed state after waking so it cannot publish a buffer after shutdown.

- **Fix:** cancel the scheduled flush with interruption (`cancel(true)`); close publishes the
  closed state, releases the credit waiter, and rechecks closure after acquiring the monitor. If a
  deployment fails partway through, rollback also cancels the tasks already accepted so no
  orphaned partial execution remains.
- **Verification:** `DistributedCheckpointRecoveryIT` kills a session-owning worker, completes
  cancellation and redeployment without waiting for the credit timeout, excludes the dead worker,
  and reaches `FINISHED` with output equal to the clean baseline.

---

## 14. User code had two class identities in the image

**Phase 7 · FIXED · visible**

The first full Compose deployment packaged LMS classes into the engine application classpath as
well as its user-code loader. Serialized job lambdas then resolved under a different loader from
their declaring job types. Libraries using thread-context discovery also could not resolve the
job's Iceberg/AWS implementation. Host tests had all dependencies on one classpath, hiding this.

**The rule:** shared engine types have one parent identity; job-owned types and libraries use one
explicit user-code classpath, including reflective discovery.

- **Fix:** `Dockerfile` and `docker/engine-entrypoint.sh` package LMS separately;
  `UserCodeClassLoader.configure` installs the same loader as the thread context loader.
- **Coverage:** `UserCodeClassLoaderTest` and the freshly rebuilt full Compose LMS deployment.

## 15. Restoration used the wrong classloader

**Phase 7 · FIXED · visible**

Deployment deserialization understood job classes, but checkpoint envelopes and transport values
still used ordinary `ObjectInputStream`. A running job therefore worked until restore needed a
job-owned state/timer-key type. This was hard to see because checkpoint creation succeeded.

**The rule:** user types can occur at every serialization boundary, not only in the submitted DAG.

- **Fix:** `UserCodeObjectInputStream.resolveClass` is shared by task copies, checkpoint readers,
  keyed state restoration, and stream-element deserialization.
- **Coverage:** distributed recovery harnesses and the containerized recovery/classpath checks.

## 16. A synchronous checkpoint ACK deadlocked sink completion

**Phase 7 · FIXED · nearly silent**

The last sink task blocked waiting for its ACK RPC to return. Receiving that ACK caused the master
to send completion back to the same sink and wait for its task-thread callback. The task could not
run the callback until its original ACK returned. Every process was alive; the checkpoint stalled
until deadlines exposed the cycle.

**The rule:** an acknowledgement must not occupy the task thread needed by a response-triggered
callback. Forward the barrier, submit the ACK asynchronously, and let the task drain callbacks.

- **Fix:** `WorkerBootstrap.acknowledgeCheckpoint` uses the asynchronous master stub.
- **Coverage:** full Compose checkpoint completion and transactional recovery acceptance.

## 17. Concurrent sink commits locked the demo catalog

**Phase 7 · FIXED · visible**

Two sink callbacks on one worker committed different tables simultaneously through the fixture
REST catalog's SQLite database. One failed with `SQLITE_BUSY`. Unit tests used an in-memory catalog
and did not exercise that shared external writer bottleneck.

**The rule:** keep the teaching catalog's transaction notifications explicit and sequential within
a worker; do not report completion before its external commits finish.

- **Fix:** `TaskManager.notifyCheckpointComplete` sorts participants and awaits each task-thread
  future in order, with bounded sink-commit and master-RPC deadlines.
- **Coverage:** the full two-table Compose fixture. This is not a general distributed SQLite
  locking strategy; an external production catalog must supply its own concurrency guarantees.

## 18. Registration preceded heartbeats by too long

**Phase 7 · FIXED · nearly silent**

A worker registered with the master, then waited for etcd lease setup before starting its
heartbeat. During busy multi-container startup that consumed the three-second liveness grace
period. A healthy newly registered worker was quarantined before it could report progress.

**The rule:** the master's liveness obligation begins at registration, not when later bootstrap
steps happen to finish.

- **Fix:** `WorkerBootstrap.start` starts heartbeats immediately after master registration.
- **Coverage:** three-worker Compose startup and process-recovery harness registration.

## 19. The catalog lost table pointers on recreation

**Phase 7 · FIXED · silent**

MinIO retained table metadata and data objects, but the REST fixture stored its SQLite catalog
pointer database in an ephemeral container `/tmp`. Compose down/up therefore retained objects
while forgetting the tables referencing them. Persisting only the warehouse was insufficient.

**The rule:** both object-store data and catalog pointers are durable state.

- **Fix:** Compose mounts `iceberg-catalog-data` at `/tmp`, the image's writable database location.
  A mount at `/catalog` was rejected by the image's non-root permissions during verification.
- **Coverage:** catalog recreation with the same volume and unchanged table metadata pointers.

## 20. The short fixture never closed its sessions

**Phase 7 · FIXED · silent**

The fixture covered about one minute, while session gaps were fifteen minutes. Kafka stayed open,
so EOF did not advance its clock. Waiting longer in wall time never made these windows complete.
All services and lag metrics looked healthy while no session rows became visible.

**The rule:** reproducible streaming fixtures must advance every active input's event-time clock
past the intended windows, rather than relying on silence as a completeness signal.

- **Fix:** checked-in click/borrow progress fixtures use explicitly selected physical Kafka
  partitions in Compose. `seed-watermark-progress.sh` does the same for the host launcher after
  both business fixtures are published.
- **Coverage:** full-stack fixture produces five session rows and three conversions.

---

## 21. A retired heartbeat stream overwrote its replacement

- **What happened:** after force-killing and restarting the master, all task threads could finish
  while the recovered job remained `RUNNING`: one worker never delivered its final heartbeat.
- **Why it was hard to see:** restored output and newer checkpoints were correct. A gRPC failure
  callback could run before `heartbeat(...)` returned, clear the stream, and then be overwritten
  by the returning stale observer. Delayed callbacks from an older stream could also clear a
  healthy replacement. Data-plane progress hid the control-plane failure.
- **Rule:** publish a stream generation before opening it; only that generation may retire itself
  using compare-and-set. Consume the reconnect reservation before opening, not afterwards.
- **Fix:** `HeartbeatClient` tracks stream generations atomically and ignores obsolete callbacks.
- **Coverage:** `HeartbeatClientTest` pins synchronous open failure, delayed old callbacks, and a
  real gRPC outage/restart reporting tasks that finished during the outage.
  `DistributedCheckpointRecoveryIT` also force-kills the real master process, observes workers
  continuing, restores all tasks, completes a newer checkpoint, and reaches `FINISHED` with output
  identical to the clean baseline.

---

## 22. A cold master never scheduled its recovered job

- **What happened:** normal Compose down/up preserved the graph, checkpoint, offsets, and table
  pointers, but the master started before worker leases existed. All 26 tasks stayed `UNKNOWN`.
- **Why it was hard to see:** metadata still said `RUNNING`; the workers, metrics endpoints, and
  catalog were healthy. `recover()` retained the graph but returned without a physical plan, and
  later control registrations only reset the failure detector. Retrying only that RPC would still
  miss the final capacity arrival: workers advertise their etcd lease after starting heartbeats.
- **Rule:** deferred recovery must react to durable worker availability, not only startup's
  snapshot or an earlier control registration. Waiting for capacity is not a failed restart.
- **Fix:** `JobMaster` retains deferred graphs and assignment cardinality, waits for sufficient
  slots, and retries on both registration and the `MasterBootstrap` worker watch. These paths
  share its monitor with `recover()`. It removes the deferred reservation before scheduling one
  whole-cut restore, including interrupted `FAILING`/`RESTARTING` states and replacement worker ids.
- **Coverage:** `JobMasterTest` covers zero workers, gradual insufficient capacity without spending
  restart attempts, all three durable states, exactly-once recovery scheduling, and control RPC
  preceding lease publication. Full-stack preserved-volume verification is recorded in
  [PHASE7-ACCEPTANCE](PHASE7-ACCEPTANCE.md).

---

## Review notes

Remaining review notes and resolved Phase 4 follow-ups.

**R1 — source-chain event-time callbacks. RESOLVED in Phase 4.** `ChainedSourceOutput` passes
watermarks through fused operators before forwarding. `DistributedCheckpointRecoveryIT` exercises
the source/filter/session pipeline through the real worker child JVMs and verifies the recovered
output against a clean baseline, covering the distributed source path.

**R2 — `InputGate.onChannelDrained` is wired but unused.** Credit is granted from the gRPC handler
after a whole buffer lands, not per element consumed. The hook exists for a more precise scheme.
Phase 4 alignment and bounded channel queues work with the current buffer-level credit model.
Revisit only if a measured backpressure or alignment issue justifies per-element credit renewal.

**R3 — Task identity parsing was corrected, but has no focused test.** `WorkerBootstrap` now
splits the final `#` from `operatorId#subtaskIndex`, so a user-supplied operator id may itself
contain `#`. The Phase 4 recovery harness exists, but it does not assert this unusual-id case;
keep a focused worker failure-reporting test on the follow-up list.

**R4 — network-partition execution fencing. ACCEPTED teaching limitation.** `TaskTracker`
cannot distinguish a dead worker from an unreachable live writer. Best-effort cancellation and
checkpoint-id fencing do not enforce a sink-side execution epoch. Process-kill tests prove their
own crash/recovery paths, not exactly-once under network partitions. A production extension must
persist execution epochs and reject external commits from superseded workers. README and DESIGN
name this boundary explicitly.

**R5 — duplicate dashboard projections. RESOLVED in Phase 7 review.** Both master and workers
publish task series. Unfiltered Grafana queries drew the same task twice, while Prometheus's
default scrape `job` label obscured the engine's job UUID. Scrapes now honor engine labels and
the four dashboard panels select worker-component series.

**R6 — finished task registrations. RESOLVED in Phase 7 review.** Bounded jobs reached
`FINISHED` but left their task map entries and flush timers registered on workers, preventing a
subsequent submission from reusing the topology's ids. `JobMaster.finishJob` now persists terminal
state, stops checkpoint coordination, and releases assignments through the existing cancellation
path. `JobMasterTest` verifies ordering, duplicate terminal-heartbeat idempotence, and that a
cleanup error does not turn a finished job into a replay.

**R7 — status JSON escaping. RESOLVED in Phase 7 review.** Job names and failure causes may
contain control characters. `StatusApi.quote` now escapes every JSON control code as well as
quotes and backslashes; `StatusApiTest` verifies both job resources against this input.

**R8 — small-fixture partition coverage. RESOLVED in Phase 7 verification.** The console
producer's round-robin mode populated only two partitions per topic in the observed small replay.
Idleness still let five sessions and three conversions complete, masking the missing coverage.
`PublishLmsFixture` now uses explicit partition ids, publishes both business inputs before either
clock fixture, and checks that progress-record count matches the partition count. Broker end
offsets are checked during full-stack acceptance, not inferred from producer configuration.

**R9 — salting on cluster submission. RESOLVED in Phase 7 review.** The local job entry point
honored the salting flag but `SubmitLmsJob` called the intentionally unsalted default builder.
Both executable entry points now select the configured mode; Compose exposes
`LMS_SESSIONS_SALTED` and Java's `lms.sessions.salted` property takes precedence. The default graph
builder remains configuration-independent. `LmsClickstreamJobTest` pins this distinction.

**R10 — bootstrap after preserving volumes. RESOLVED in Phase 7 review.** Compose down/up
recreates one-shot services while etcd, Kafka, and Iceberg retain their histories. Blindly
submitting another same-id topology or reseeding business input would collide with restored tasks
or append another replay. Compose now reuses one nonterminal LMS graph and skips fixture seeding
only when every partition meets its complete fixture baseline; incomplete bootstrap fails closed
with explicit recovery/reset guidance. `ComposeBootstrapTest` pins the policy. These checks are
for the single local bootstrap, not a concurrent/distributed ingestion transaction protocol.

---

## Adding an entry

Keep the format: what happened, why it was hard to see, the rule that prevents it, and where the
fix and its test live. The "why it was hard to see" line is the one worth writing carefully — it
is what makes this log useful to a reader of the article rather than only to us.
