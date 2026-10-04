# Historical local pause checkpoint — 2026-10-04

> Superseded by the user's resume request and completed Phase 7 verification. See
> [PHASE7-ACCEPTANCE](PHASE7-ACCEPTANCE.md) for the final result. The original pause notes below
> are retained as history, not current instructions.

Development is **paused at the user's request** until they ask to resume. Do not
automatically restart services or tests. Phase 7 is not yet accepted as complete.

> Historical checkpoint: the user subsequently authorized resuming Phase 7, verifying it,
> committing, and pushing the existing branch. The pause above no longer applies to that resumed
> work. Final verification is recorded separately; the notes below preserve the original boundary.

## Saved state

- Branch: `codex/complete-phases-3-7`; do not merge into `main` before the user's
  demo/manual confirmation pass.
- Remote was last pushed through Phase 6 (`5175487`). Prior local checkpoint:
  `bb3f3ca`. This pause checkpoint saves the current Phase 7 changes locally.
- New work: real watermark propagation through heartbeat/REST/Prometheus,
  per-channel backpressure detection, task-metric cleanup, stronger recovery and
  hot-key process tests, a real late-event pipeline harness, deterministic fixture
  watermark advancement, and persistent Iceberg REST catalog storage.

## Verification boundaries

- Passed: 22 focused runtime, worker, and master observability tests, including
  restored watermarks, heartbeat presence, full-channel backpressure, and metric
  identity/cleanup.
- Before the newest observability changes, the acceptance Compose stack built
  and ran the LMS job without restarts. Observed: 14 successful checkpoints,
  zero failed checkpoints, Kafka lag zero, four healthy Prometheus targets,
  healthy Grafana, five browse-session rows and three conversion rows committed
  to Iceberg. This is not proof of the final source tree/image.
- Hot-key integration test compiled. Its initial fixture serialization bug was
  corrected; the rerun was interrupted during the unsalted stage when the user
  paused. No passing end-to-end hot-key result is claimed.
- New master-loss and transactional Iceberg worker-loss process tests are saved
  but not run. The Iceberg comparison uses real REST/MinIO/Parquet and the actual
  sink, with a small canonical-row schema; production schema has separate tests.
- New `LateEventPipelineAcceptanceTest` and all revised demo scripts still need
  execution. Full `gradlew check` has not been run against this checkpoint.
- Local build evidence/logs remain under `lms-job/build/demo-4` and, when the
  recovery tests run, `lms-job/build/demo-evidence`. These are generated artifacts,
  not source files, and may be removed by `clean`.

## Shutdown and preserved data

- Compose project `lms-phase7-acceptance` was brought down **without `-v`**.
  Its containers and network were removed; all six named volumes were verified
  present: etcd, Kafka, MinIO, Iceberg catalog, Prometheus, and Grafana.
- The active hot-key Gradle test, its master/worker child JVMs, and its ephemeral
  Testcontainers were stopped. No project test/engine JVMs remained afterward.
- Unrelated `releaseguard` containers and editor language servers were untouched.
- Original older project volumes were not removed either.
- Acceptance ports had overrides because unrelated services occupy the defaults:

  ```powershell
  $env:MASTER_STATUS_HOST_PORT='28080'
  $env:WORKER1_METRICS_HOST_PORT='28081'
  $env:WORKER2_METRICS_HOST_PORT='28082'
  $env:WORKER3_METRICS_HOST_PORT='28083'
  $env:GRAFANA_HOST_PORT='23000'
  # Only after the user resumes:
  docker compose -p lms-phase7-acceptance up -d --build
  ```

  Rebuild the engine image: the saved image predates the newest metrics changes.
  Reusing preserved volumes retains prior topics/checkpoints/tables; use a new
  Compose project name for a clean deterministic acceptance run rather than
  deleting the preserved volumes.

## Resume order

1. Read this checkpoint, current diff, and `CLAUDE.md`. Keep Phase 7 in progress.
2. Run one host Gradle task at a time. Finish/debug `DistributedHotKeyIT`, the
   expanded `DistributedCheckpointRecoveryIT`, and `LateEventPipelineAcceptanceTest`.
3. Run all four demo scripts using Git Bash (`C:/Program Files/Git/bin/bash.exe`,
   not the Windows WSL `bash`), checking Windows line endings and evidence output.
4. Rebuild and reverify the complete Compose stack, API metrics/dashboard, and
   catalog persistence. The REST SQLite catalog is mounted at `/tmp` because the
   fixture image's non-root process could not write a newly mounted `/catalog`.
5. Reconcile README, DESIGN, ARCHITECTURE and BUG-LOG with verified behavior.
   Current docs prematurely claim completion and contain stale statements about
   missing heartbeat watermarks and lack of process-level sink recovery tests.
   Document the teaching engine's network-partition fencing limitation explicitly.
6. Add runtime-discovered bug records: classloader/checkpoint restoration,
   synchronous ACK reentrant sink-completion deadlock, concurrent SQLite commits,
   registration/heartbeat startup gap, ephemeral catalog pointer DB, and fixtures
   whose timestamps never advance far enough to close sessions. Some fixes were
   already in the previous local checkpoint but documentation is still pending.
7. Run full checks and diff review; only then make the final Phase 7 commit and
   arrange the user's demo/manual review. Keep the existing branch; no main merge.

Other remaining audits: original article link (do not invent a LinkedIn URL), API
Javadoc coverage, zero external engine-api dependencies, and final phase
acceptance consistency with the original companion PDF.
