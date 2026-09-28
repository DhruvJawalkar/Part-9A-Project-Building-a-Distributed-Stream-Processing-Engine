# Iceberg demo surface

The Phase 6 demo uses the official `apache/iceberg-rest-fixture` image and MinIO. Start the
dependencies, then create the tables explicitly:

```bash
docker compose up -d
./demos/iceberg/init-schema.sh
```

`localhost:8181` is the REST catalog endpoint for host-launched workers. The REST service uses
`http://minio:9000` because it runs inside the Compose network. The job submitter defaults to
`ICEBERG_S3_ENDPOINT=http://localhost:9000`, captures the catalog/S3 settings in the serialized job
graph, and workers construct clients from those captured values. Set `ICEBERG_*` variables on the
`./gradlew :lms-job:submitToCluster` command to override them.

`schema.sql` is the readable SQL contract. The `*.schema.json`, `*.partition-spec.json`, and
`*.table.json` files are the equivalent REST payloads. Both tables intentionally use Iceberg's
unpartitioned spec for the first sink integration: it keeps the checkpoint-to-append path
legible and matches the current sink's supported table shape. Session and conversion timestamps are
event-time values with millisecond precision in the job models; Iceberg's REST type is the
microsecond-capable `timestamp`, which preserves those values without rounding.

## Acceptance coverage

`IcebergSinkAcceptanceTest` deliberately pauses after writing files and before checkpoint
notification. With Iceberg's in-memory catalog and local file IO, it verifies these probes:

1. Start one checkpoint interval, write rows, and pause before completion. Query both tables and
   assert that the interval contributes zero rows and no current snapshot manifests.
2. Release `notifyCheckpointComplete(checkpointId)`. Query again and assert that all rows from the
   interval appear in one new snapshot, with the prior snapshot still readable.
3. Replace a sink after its pre-commit files are closed but before notification. Assert that those
   Parquet files remain outside every reachable snapshot, then restore/replay and assert each
   logical row occurs exactly once.
4. Run the same fixture at two checkpoint intervals. Record first-visible-row latency and the
   number of newly added data files per minute; the shorter interval should improve freshness and
   increase small-file count.

The automated harness intentionally isolates the transaction protocol from network services. The
Compose REST catalog and MinIO setup above is an executable demo surface, not yet an automated
process-kill REST/S3 acceptance test. Keep that distinction when interpreting the evidence: an
orphan is expected after loss, while a duplicate visible row is not.

With the three demo services and schemas running, exercise the real REST catalog, `S3FileIO`, and
MinIO writer path with:

```bash
./gradlew -DicebergRestSmoke=true :engine-connectors:test \
  --tests dev.dhruv.streaming.connectors.iceberg.IcebergRestMinioSmokeTest
```

This opt-in smoke test publishes a unique conversion row through a real checkpoint completion. It
does not simulate a process kill; the self-contained acceptance test remains the recovery-protocol
proof.
