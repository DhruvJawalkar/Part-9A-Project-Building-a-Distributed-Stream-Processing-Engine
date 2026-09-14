#!/usr/bin/env bash
#
# Publishes the click fixture to lms.catalog.clicks.
#
# The fixture is a fixed file rather than generated traffic on purpose. Every demo in this
# project has to be reproducible -- the determinism test in Phase 4 compares a recovered run
# against a clean one byte for byte, which is only a meaningful comparison if both runs read
# exactly the same input.
#
#   ./demos/seed-clicks.sh                     publish the standard fixture
#   ./demos/seed-clicks.sh path/to/other.jsonl publish a different one
#
set -euo pipefail

# Git Bash on Windows rewrites arguments that look like Unix paths into Windows ones, which
# mangles the in-container path below into something Docker cannot find. Harmless elsewhere.
export MSYS_NO_PATHCONV=1

FIXTURE="${1:-$(dirname "$0")/fixtures/clicks.jsonl}"
TOPIC="lms.catalog.clicks"
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
echo "  The job prints one line per surviving click event."
echo "  Of the $(wc -l < "$FIXTURE") events in the fixture, BotFilter drops five:"
echo "    3 from bot-* member ids, 1 with a blank member id, 1 with no event time."
echo "  Expect 15 lines on the job's terminal."
echo

# --property parse.key + key.separator is what puts a member's events in one partition.
# Kafka hashes the key to pick a partition, so per-member ordering holds within a partition,
# which is what the session aggregator will rely on in Phase 3.
sed 's/^{"memberId":"\([^"]*\)".*/\1\t&/' "$FIXTURE" \
  | docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:19092 \
      --topic "$TOPIC" \
      --property parse.key=true \
      --property key.separator=$'\t'

echo "published $(wc -l < "$FIXTURE") events to ${TOPIC}"
