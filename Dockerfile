# Build all three engine entry points once, then reuse one small runtime image for the
# master, workers, and job submitter. The LMS distribution stays separate from the engine
# distributions so that user code is resolved through UserCodeClassLoader instead of being
# accidentally loaded by the engine's application class loader.
FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace
COPY . .

RUN --mount=type=cache,target=/root/.gradle chmod +x gradlew \
    && ./gradlew -Dorg.gradle.java.home=/opt/java/openjdk --no-daemon \
        :engine-master:installDist \
        :engine-worker:installDist \
        :lms-job:installDist \
        :lms-job:submitClasses

FROM eclipse-temurin:21-jre AS runtime

WORKDIR /opt/engine
COPY --from=build /workspace/engine-master/build/install/engine-master/ master/
COPY --from=build /workspace/engine-worker/build/install/engine-worker/ worker/
COPY --from=build /workspace/lms-job/build/install/lms-job/ lms/
COPY --from=build /workspace/lms-job/build/classes/java/submit/ submit/classes/
COPY docker/engine-entrypoint.sh ./engine-entrypoint.sh

# The wrapper constructs JOB_CLASSPATH from the separately packaged job distribution. This is
# the local teaching-project equivalent of a production engine shipping a user-code artifact.
RUN chmod +x engine-entrypoint.sh \
    master/bin/engine-master worker/bin/engine-worker lms/bin/lms-job

EXPOSE 8080 8081 9090 9091 9191
