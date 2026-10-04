#!/usr/bin/env bash
# Close the standard fixture's windows without mistaking wall-clock silence for event time.
set -euo pipefail
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Advance both input clocks after publishing clicks and borrows."
echo "Each of four partitions receives a future event; fixture-clock is not a business session."
cd "$ROOT"
./gradlew :lms-job:publishFixture -PfixtureProgressOnly=true
