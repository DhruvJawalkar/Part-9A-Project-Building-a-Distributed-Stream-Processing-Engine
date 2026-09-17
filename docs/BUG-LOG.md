# Bug log

A running record of bugs found while building the engine, kept so they can be reviewed in one
pass rather than reconstructed from commit messages later.

Every entry here was found by **running the thing**, not by a test failing. That is the point of
keeping the log: the tests came second in each case, written to pin a bug that had already
happened. Which bugs a design lets you make is worth knowing.

**Status key:** `FIXED` — fixed and pinned by a test · `OPEN` — known, not yet addressed ·
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

### The pattern worth noticing

**Four of six produced no error at all.** No exception, no failed health check, no red log line —
just a job that was up, reported as `RUNNING`, and quietly not doing its work. Two of those (#3,
#5) would have been reported as "the engine is slow" or "the windows are wrong" by anyone who did
not already know where to look.

That is the characteristic failure mode of a distributed stream processor, and it is the same
shape as Demo 4 in Phase 7: *it fails while looking entirely healthy*. Worth remembering when
writing the article — these are not embarrassments to hide, they are the argument for why the
mechanisms exist.

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

## Review notes

Things noticed while fixing the above that are not bugs yet, but should be looked at when the log
is reviewed.

**R1 — `ChainedSourceOutput` timestamp handling is untested.** It mirrors `OperatorChain`'s
`currentTimestamp` logic, which is where bug #5 lived. The code looks right; nothing proves it.
Phase 3 is the natural time to cover it, because that is when a chained source first feeds
something that cares about event time.

**R2 — `InputGate.onChannelDrained` is wired but unused.** Credit is granted from the gRPC handler
after a whole buffer lands, not per element consumed. The hook exists for a more precise scheme.
Decide in Phase 4 whether alignment makes the finer granularity worth having, or remove it.

**R3 — Task failure reporting parses the task key with `split("#")`.** `WorkerBootstrap` splits a
`operatorId#subtaskIndex` string to rebuild a `TaskId`. An operator id containing `#` would break
it. Operator ids are user-supplied, so this is reachable; it just has not been reached.

**R4 — `TaskTracker` cannot distinguish a dead worker from an unreachable one.** Not fixable —
this is the failure detector problem — but the *consequence* changes by phase. In Phase 2 a
partitioned worker can corrupt nothing, so assuming the worst is safe. From Phase 6 a partitioned
worker could still be writing to Iceberg. Revisit then.

---

## Adding an entry

Keep the format: what happened, why it was hard to see, the rule that prevents it, and where the
fix and its test live. The "why it was hard to see" line is the one worth writing carefully — it
is what makes this log useful to a reader of the article rather than only to us.
