#!/usr/bin/env bash
#
# Starts a master and three workers as separate processes.
#
# What to watch for:
#   1. Three workers register and appear in etcd under /workers/.
#   2. The session and conversion branches are spread across all three workers.
#
# Ports: master 7000, worker control 7001-7003, worker data 7101-7103. Deliberately clear of
# 9092, which Kafka takes.
#   3. Worker logs show records arriving from other processes.
#
#   ./demos/run-cluster.sh          start the cluster
#   ./demos/run-cluster.sh stop     stop everything
#
# Then submit the LMS job with:
#   ./gradlew :lms-job:submitToCluster
#
set -uo pipefail

# Git Bash on Windows rewrites arguments that look like Unix paths. Harmless elsewhere.
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOGS="$ROOT/demos/logs"
PIDS="$LOGS/pids"

stop_cluster() {
  if [ -f "$PIDS" ]; then
    while read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$PIDS"
    rm -f "$PIDS"
  fi
}

if [ "${1:-start}" = "stop" ]; then
  stop_cluster
  echo "cluster stopped"
  exit 0
fi

mkdir -p "$LOGS"
stop_cluster
: > "$PIDS"

echo "building..."
(cd "$ROOT" && ./gradlew -q installDist) || { echo "build failed" >&2; exit 1; }

# The workers were compiled with no knowledge of this job, so they have to be told where its
# classes are. A production engine ships the job's JAR as part of submission; this project takes
# the shortcut of naming the classpath at worker startup. See UserCodeClassLoader for why a
# process that runs arbitrary user code needs a separate loader at all.
#
# Two things differ between Git Bash and the Windows JVM it launches, and both bite here:
# the path separator (';' vs ':') and the path format itself ('C:/...' vs '/c/...'). The JVM
# understands neither of Git Bash's conventions, so both are converted.
JOB_LIB="$ROOT/lms-job/build/install/lms-job/lib"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    CP_SEP=';'
    JOB_CLASSPATH=$(find "$JOB_LIB" -name '*.jar' -exec cygpath -m {} \; | paste -sd "$CP_SEP" -)
    ;;
  *)
    CP_SEP=':'
    JOB_CLASSPATH=$(find "$JOB_LIB" -name '*.jar' | paste -sd "$CP_SEP" -)
    ;;
esac
export JOB_CLASSPATH

echo "starting master on :7000"
# The master needs the job's classes too: it deserializes the submitted graph in order to
# compile it, and that graph holds the user's own operators.
MASTER_PORT=7000 ETCD_ENDPOINTS=http://localhost:2379 \
  CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-10000}" \
  CHECKPOINT_TIMEOUT_MS="${CHECKPOINT_TIMEOUT_MS:-30000}" \
  RESTART_MAX_ATTEMPTS="${RESTART_MAX_ATTEMPTS:-3}" \
  RESTART_DELAY_MS="${RESTART_DELAY_MS:-1000}" \
  JOB_CLASSPATH="$JOB_CLASSPATH" \
  "$ROOT/engine-master/build/install/engine-master/bin/engine-master" \
  > "$LOGS/master.log" 2>&1 &
echo $! >> "$PIDS"
sleep 3

for i in 1 2 3; do
  rpc=$((7000 + i))
  data=$((7100 + i))
  echo "starting worker-$i (control :$rpc, data :$data)"
  WORKER_ID="worker-$i" \
  WORKER_HOST=localhost \
  WORKER_RPC_PORT=$rpc \
  WORKER_DATA_PORT=$data \
  WORKER_SLOTS="${WORKER_SLOTS:-12}" \
  MASTER_HOST=localhost \
  MASTER_PORT=7000 \
  ETCD_ENDPOINTS=http://localhost:2379 \
  MINIO_ENDPOINT=http://localhost:9000 \
  MINIO_ACCESS_KEY=minioadmin \
  MINIO_SECRET_KEY=minioadmin \
  MINIO_BUCKET=stream-checkpoints \
  JOB_CLASSPATH="$JOB_CLASSPATH" \
    "$ROOT/engine-worker/build/install/engine-worker/bin/engine-worker" \
    > "$LOGS/worker-$i.log" 2>&1 &
  echo $! >> "$PIDS"
done

echo "waiting for workers to register..."
sleep 6

echo
echo "workers in etcd:"
docker exec lms-etcd etcdctl --endpoints=http://localhost:2379 \
  get --prefix /workers/ --keys-only 2>/dev/null | grep -v '^$' || echo "  (none)"

echo
echo "logs are in demos/logs/"
echo "submit the job with:  ./gradlew :lms-job:submitToCluster"
echo "stop the cluster with: ./demos/run-cluster.sh stop"
