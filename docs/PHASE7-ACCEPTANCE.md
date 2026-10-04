# Phase 7 acceptance — 2026-10-04

**PASS — all seven phases implemented and verified.** This report records implementation acceptance, not the user's
subsequent manual/demo approval or permission to merge into `main`.

The final all-module run executed 48 Gradle tasks in 5 min 41 s: **237 tests passed, zero failures
or errors, one expected opt-in smoke-test skip** (238 cases across 52 JUnit suites). The skipped
`IcebergRestMinioSmokeTest` writes into an externally bootstrapped table; ordinary checks instead
run the automated REST/MinIO process-crash Iceberg proof. API Javadoc generation passed (with
non-blocking member-documentation warnings); the API runtime dependency report is empty.

## Reproducible demonstrations

All four scripts were executed from Git Bash on Windows against the checked-in fixtures.
They use assertion-backed harnesses, not an operator's interpretation of printed logs.

| Script | Observed result |
|---|---|
| `demos/demo-1-worker-loss.sh` | PASS: session-owner process kill restores one distributed cut; output equals clean replay. Sink-owner kill leaves an uncommitted Parquet orphan; actual REST/MinIO Iceberg scans contain the clean run's two unique rows exactly once. |
| `demos/demo-2-master-loss.sh` | PASS: a worker processes another click while the master process is down; a replacement restores all five tasks, completes a newer checkpoint, reaches `FINISHED`, and produces the clean run's bytes. |
| `demos/demo-3-late-event.sh` | PASS: source watermark is 895,000 ms. Default policy drops/counts the older click; 900,000 ms allowed lateness includes it in the retained session, changing the count from one to two. |
| `demos/demo-4-hot-key.sh` | PASS: real three-worker execution of the production session topology produces identical session bytes in unsalted and salted modes. REST and worker Prometheus samples measure actual queues and distribution. |

Final full-suite Demo 4 harness run (16,000 clicks, identical controlled per-click work in both modes):

| Signal | Unsalted | Salted |
|---|---:|---:|
| Clicks per local subtask | 0 / 0 / 0 / 16,000 | 4,924 / 3,024 / 4,123 / 3,929 |
| Backpressured observations | 169 / 188 | 0 / 83 |
| Peak busiest input queue | 2,048 | 532 |
| Mean busiest input queue | 1,916 | 103 |
| Time to drain business input | 56.3 s | 26.1 s |

This is a controlled teaching workload, not a general throughput benchmark. The hot key is a
member (the actual session key), not the article's catalog-item example. Salting balances work
approximately, not perfectly; the global stage receives 16 compact fragments rather than all
16,000 clicks. The test requires materially lower sustained pressure, not zero transient pressure
on every machine.

The recovery Iceberg table stores a canonical complete session row in one string column to make
comparison exact. Production LMS table mappings are separately exercised by sink tests and the
full Compose fixture. The late-data demonstration revises a retained session; it does not reopen
already-emitted historical Iceberg rows.

## Full-stack checks

The verification Compose project is `lms-phase7-verified`. Host ports 28080 (master),
28081–28083 (workers), and 23000 (Grafana) avoid an unrelated running application. Defaults in
the README remain 18080, 18081–18083, and 13000.

Verified with the final cold-start recovery correction:

- Full runtime image builds and bootstrap starts one master plus three workers and 26 tasks.
- Every input partition is populated explicitly: click end offsets are 6 / 6 / 6 / 6 and borrow
  offsets are 2 / 2 / 2 / 2. Source lag reaches zero for all eight partitions.
- All 26 task watermarks are established; periodic checkpoints complete.
- Production Iceberg snapshots contain five browse-session rows and three conversion rows.
- Prometheus has four healthy scrape targets. Task series retain the engine job UUID and worker
  component label. Grafana provisions exactly four panels: source lag, checkpoint duration and
  alignment, records-in by subtask, and state size by subtask.
- Recreating the catalog retains its SQLite pointer database and table metadata locations.
- Normal Compose down/up preserves those table pointers and Kafka offsets. The job bootstrap
  reuses the same job UUID; the fixture bootstrap explicitly skips replay.
- Cold restart restores the same job `fa0880b1-075a-49e7-80f5-7cb1a115f7da` from checkpoint 67.
  After waiting for workers, it returns to `RUNNING` with one restart, all 26 heartbeats and
  established watermarks, and newer checkpoints 68/69 completed with zero failures. Input offsets
  and table rows remain unchanged. Regression tests also cover interrupted `FAILING`/`RESTARTING`
  states and control registration preceding the worker's etcd lease.

## Evidence and rerun commands

```bash
./gradlew check --rerun-tasks --max-workers=2
./gradlew :engine-api:javadoc :engine-api:dependencies --configuration runtimeClasspath
./demos/demo-1-worker-loss.sh
./demos/demo-2-master-loss.sh
./demos/demo-3-late-event.sh
./demos/demo-4-hot-key.sh
```

JDK 21 and Docker are required. Run Docker-backed Gradle invocations serially on this Windows
checkout: their child-process artifacts share build paths. Dependency JAR producers are wired
into the integration pathing JAR so a focused rerun cannot silently execute stale master/worker
code. Every script forces its acceptance task to execute rather than accepting an up-to-date test.

Generated evidence is intentionally ignored by Git and regenerated by these commands:

- `lms-job/build/demo-evidence/`: clean/recovered rows, process logs, final REST statuses,
  committed/orphan Iceberg paths, and Demo 1/2 stdout.
- `lms-job/build/demo-3/report.txt`: late-data rows, watermark, and counters.
- `lms-job/build/demo-4/`: fixed generated click replay, both outputs, JSONL REST samples,
  worker Prometheus scrapes, process logs, and summary report.
- Each module's `build/reports/tests/` and `build/test-results/`: Gradle/JUnit results.

## Acceptance boundary

The temporary verification stack was stopped with `docker compose down`, without `-v`.
Its volumes and generated local evidence are preserved; unrelated applications were not stopped.
To restart that same project for manual inspection in Git Bash:

```bash
MASTER_STATUS_HOST_PORT=28080 WORKER1_METRICS_HOST_PORT=28081 \
WORKER2_METRICS_HOST_PORT=28082 WORKER3_METRICS_HOST_PORT=28083 \
GRAFANA_HOST_PORT=23000 docker compose -p lms-phase7-verified up -d --build
```

Then inspect <http://localhost:28080/jobs>, <http://localhost:9090>, and
<http://localhost:23000> (Grafana `admin` / `admin`). The four demo scripts remain independent of
this Compose project. Do not discard volumes unless you deliberately want a clean replay.

The implementation remains a teaching engine. See [README limitations](../README.md#known-limitations)
for missing network-partition execution-epoch fencing, concurrent same-operator-id job isolation,
terminal checkpoints for bounded transactional jobs, and historical late-row upserts. Process-kill
tests do not establish partition-safe exactly-once semantics. Orphan objects remain physically
present and require ordinary object-store maintenance.

Design, component integration, event-time/state rules, master-worker communication, checkpoint
coordination, and failure recovery are documented in [DESIGN](DESIGN.md) and
[ARCHITECTURE](ARCHITECTURE.md); runtime-discovered issues are retained in [BUG-LOG](BUG-LOG.md).
The supplied article PDF is linked from the README and copied unchanged (SHA-256
`772277c85d04f53f96a3b9cb3e91877de9f81f5a3582396673fffc036808d14c`).

Keep `codex/complete-phases-3-7` separate from `main` until the user's manual/demo confirmation.
