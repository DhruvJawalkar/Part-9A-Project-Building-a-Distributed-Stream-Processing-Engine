#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$ROOT/demos/fixtures/hot-key.properties"

echo "Demo 4 — one member becomes a hot key"
echo "Fixture: $FIXTURE"
echo "What to watch:"
echo "  - unsalted keyBy(memberId) sends all 16,000 records to one subtask; siblings are idle;"
echo "  - that subtask exceeds the fixture's service capacity and is marked backpressured;"
echo "  - -Dlms.sessions.salted=true adds a 16-way local stage and flattens local work;"
echo "  - the global stage merges compact fragments and produces the same SessionRow."
echo "Proves: aggregate throughput hides skew, while per-subtask metrics expose and verify the fix."
echo "Article: partitioning, skew, and backpressure (Part 9A, §§5.3 and 9)."
echo

cd "$ROOT"
./gradlew :lms-job:test \
  --tests 'dev.dhruv.streaming.lms.SaltedSessionAggregatorTest.assignsOneHotMemberAcrossEverySaltWithBoundedSkew' \
  --tests 'dev.dhruv.streaming.lms.SessionPipelineAcceptanceTest.saltedTwoPhaseSessionsProduceTheSameClosedRowsAsTheSingleKeyPath'
