#!/bin/sh
set -eu

# Keep the job JAR and its dependencies out of the engine application classpath. The engine's
# UserCodeClassLoader is parent-first, so shared engine API types retain one identity while LMS
# operators, records, and serialized lambdas resolve from this explicit deployment classpath.
JOB_CLASSPATH=""
for jar in /opt/engine/lms/lib/*.jar; do
  if [ -z "$JOB_CLASSPATH" ]; then
    JOB_CLASSPATH="$jar"
  else
    JOB_CLASSPATH="$JOB_CLASSPATH:$jar"
  fi
done
export JOB_CLASSPATH

case "${1:-}" in
  master)
    exec /opt/engine/master/bin/engine-master
    ;;
  worker)
    exec /opt/engine/worker/bin/engine-worker
    ;;
  *)
    echo "usage: engine-entrypoint.sh {master|worker}" >&2
    exit 2
    ;;
esac
