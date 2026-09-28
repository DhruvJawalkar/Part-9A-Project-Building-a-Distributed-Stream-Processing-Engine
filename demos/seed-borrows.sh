#!/usr/bin/env bash
# Publishes the deterministic borrow fixture used by the Phase 5 interval join.
set -euo pipefail

export MSYS_NO_PATHCONV=1

FIXTURE="${1:-$(dirname "$0")/fixtures/borrows.jsonl}"
TOPIC="lms.catalog.borrows"
CONTAINER="lms-kafka"

if [ ! -f "$FIXTURE" ]; then
  echo "fixture not found: $FIXTURE" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "Kafka is not running. Start it with: docker compose up -d" >&2
  exit 1
fi

echo "What to watch for:"
echo "  Three borrows match RESULT_CLICK events for the same member and catalog item."
echo "  loan-too-late is 30 minutes and 1 millisecond beyond its click and must not match."
echo "  Publish clicks first or borrows first: the result set is identical because both sides buffer."
echo

sed 's/^{"memberId":"\([^"]*\)".*/\1\t&/' "$FIXTURE" \
  | docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:19092 \
      --topic "$TOPIC" \
      --property parse.key=true \
      --property key.separator=$'\t'

echo "published $(wc -l < "$FIXTURE") events to ${TOPIC}"
