#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Demo 2 — master dies after a durable checkpoint"
echo "Fixture: demos/fixtures/recovery-clicks.txt, replayed by real worker processes."
echo "What to watch:"
echo "  - the master is force-killed after a checkpoint in etcd/MinIO;"
echo "  - a worker processes the next fixture click while the master is down;"
echo "  - a fresh master reads the graph, assignments, state, and checkpoint from metadata;"
echo "  - it fences the old execution, restores every task, and replays completion idempotently;"
echo "  - the recovered job finishes with bytes identical to a clean replay."
echo "Proves: etcd is the durable control-plane memory; master RAM is not authoritative."
echo "Article: master/worker coordination and recovery (Part 9A, §§7 and 10.3)."
echo

cd "$ROOT"
./gradlew :lms-job:integrationTest \
  --tests 'dev.dhruv.streaming.lms.DistributedCheckpointRecoveryIT.killedMasterRestoresWholeJobWhileWorkersKeepProcessing'
