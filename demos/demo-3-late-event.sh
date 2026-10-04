#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$ROOT/demos/fixtures/late-event.properties"

echo "Demo 3 — an event arrives behind the watermark"
echo "Fixture: $FIXTURE"
echo "What to watch:"
echo "  - zero allowed lateness increments late-session-events and leaves the row unchanged;"
echo "  - a 15-minute allowance accepts the same event and revises the still-open session;"
echo "  - accepted-late-session-events distinguishes a revision from a drop;"
echo "  - build/demo-3/report.txt contains actual runtime counters and the single output row."
echo "Proves: watermark progress is an explicit correctness policy, not just a clock."
echo "Article: event time, watermarks, and late data (Part 9A, §§6.2–6.4)."
echo

cd "$ROOT"
./gradlew :lms-job:test \
  --rerun-tasks \
  --tests 'dev.dhruv.streaming.lms.LateEventPipelineAcceptanceTest.replayDropsOrRevisesTheOpenSessionBehindAnActualWatermark'
