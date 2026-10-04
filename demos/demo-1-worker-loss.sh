#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Demo 1 — worker dies while a member session is open"
echo "Fixture: demos/fixtures/recovery-clicks.txt."
echo "What to watch:"
echo "  - the selected session owner is force-killed after a completed checkpoint;"
echo "  - the master cancels every task, not just the dead worker's task;"
echo "  - surviving workers receive a whole-job redeployment from the MinIO checkpoint;"
echo "  - recovered output is byte-identical to a clean replay."
echo "  - a second worker kill leaves an uncommitted Parquet orphan in MinIO;"
echo "  - the recovered Iceberg table contains exactly the clean run's rows, once."
echo "Proves: checkpoint state, source offsets, timers, and recovery form one consistent cut."
echo "Article: checkpointing and failure recovery (Part 9A, §§10.1–10.3)."
echo

cd "$ROOT"
./gradlew :lms-job:integrationTest \
  --tests 'dev.dhruv.streaming.lms.DistributedCheckpointRecoveryIT.killedSessionWorkerRestartsWholeJobFromPortableCheckpoint' \
  --tests 'dev.dhruv.streaming.lms.DistributedCheckpointRecoveryIT.killedSinkWorkerLeavesOrphanAndRecoversExactlyOnceIcebergRows'
