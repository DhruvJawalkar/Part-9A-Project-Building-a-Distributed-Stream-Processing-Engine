#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Demo 2 — master dies after a durable checkpoint"
echo "Fixture: JobMasterTest creates a fixed six-task graph and a completed checkpoint."
echo "What to watch:"
echo "  - a fresh master reads the graph, assignments, state, and checkpoint from metadata;"
echo "  - it fences the old execution, restores every task, and replays completion idempotently;"
echo "  - the job returns to RUNNING rather than continuing from mixed task epochs."
echo "Proves: etcd is the durable control-plane memory; master RAM is not authoritative."
echo "Article: master/worker coordination and recovery (Part 9A, §§7 and 10.3)."
echo

cd "$ROOT"
./gradlew :engine-master:test \
  --tests 'dev.dhruv.streaming.master.JobMasterTest.restartedMasterRestoresWholeJobAndReplaysCompletion'
