#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$ROOT/demos/fixtures/hot-key.properties"

echo "Demo 4 — one member becomes a hot key"
echo "Fixture: $FIXTURE"
echo "What to watch:"
echo "  - three actual worker JVMs execute the production LMS session DAG;"
echo "  - unsalted keyBy(memberId) sends all 16,000 records to one subtask; siblings are idle;"
echo "  - identical controlled per-record work fills a real bounded input channel;"
echo "  - -Dlms.sessions.salted=true adds a 16-way local stage and flattens local work;"
echo "  - REST heartbeat counters and worker Prometheus scrapes prove the distribution;"
echo "  - the global stage merges compact fragments and produces the same SessionRow."
echo "Proves: aggregate throughput hides skew, while per-subtask metrics expose and verify the fix."
echo "Article: partitioning, skew, and backpressure (Part 9A, §§5.3 and 9)."
echo

cd "$ROOT"
./gradlew :lms-job:integrationTest \
  --tests 'dev.dhruv.streaming.lms.DistributedHotKeyIT' --rerun
echo "Measured proof and raw task/Prometheus samples: lms-job/build/demo-4/"
cat "$ROOT/lms-job/build/demo-4/report.txt"
