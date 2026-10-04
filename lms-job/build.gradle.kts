plugins {
    application
}

dependencies {
    // The job's compile surface is the API plus the connectors, and nothing else.
    //
    // CLAUDE.md section 3 originally said "engine-api ONLY". That made PDF section 5.4's
    // KafkaSource.of("lms.catalog.clicks", ClickEvent.class) impossible to write, because
    // KafkaSource lives in engine-connectors. The rule was amended rather than the article
    // snippet; see the amendment note in CLAUDE.md section 3.
    implementation(project(":engine-api"))
    implementation(project(":engine-connectors"))

    // NOTE what is absent: engine-runtime. The discipline that actually matters -- the job must
    // never reach into the engine's internals -- is still enforced mechanically, because
    // engine-connectors declares engine-runtime as `implementation` rather than `api`, so it
    // does not leak onto this module's compile classpath. If a job class ever fails to compile
    // because it cannot see a runtime type, that is the rule working, not a build problem.
    runtimeOnly(project(":engine-runtime"))
    runtimeOnly(libs.logback)

    // Phase 4's recovery acceptance test deliberately assembles real task instances. This is
    // test scope only: user job code still cannot compile against runtime internals.
    testImplementation(project(":engine-runtime"))

    // Phase 4's process-level recovery proof starts the same master and worker entry points as
    // the demo cluster. These remain integration-test-only so user job code retains its narrow
    // API + connectors compile surface.
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit)
}

application {
    mainClass.set("dev.dhruv.streaming.lms.LmsClickstreamJob")
}

// A second entry point that submits the same job to a running cluster rather than executing it
// in this process. Kept in its own source set so that the job module proper still compiles
// against engine-api and engine-connectors alone -- the submitter needs the master's client,
// and letting that into the job's main classpath would quietly undo the rule above.
sourceSets {
    create("submit") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
    create("integrationTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }
}

configurations["submitImplementation"].extendsFrom(configurations.implementation.get())
configurations["submitRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())
// Bootstrap policy tests exercise the submitter utilities without exposing their dependencies
// to the main user-job compile surface.
sourceSets.test {
    compileClasspath += sourceSets["submit"].output + sourceSets["submit"].compileClasspath
    runtimeClasspath += sourceSets["submit"].output + sourceSets["submit"].runtimeClasspath
}
configurations["integrationTestImplementation"]
    .extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"]
    .extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    add("submitImplementation", project(":engine-master"))
    add("submitImplementation", libs.kafka.clients)
    add("submitImplementation", libs.jackson)
    add("integrationTestImplementation", project(":engine-master"))
    add("integrationTestImplementation", project(":engine-worker"))
    // Read real REST/MinIO Iceberg output after a worker process is force-killed.
    add("integrationTestImplementation", libs.iceberg.core)
    add("integrationTestImplementation", libs.iceberg.parquet)
    add("integrationTestImplementation", libs.parquet.avro)
    add("integrationTestImplementation", libs.minio)
    add("integrationTestImplementation", libs.hadoop.mapreduce.client.core)
}

tasks.register<JavaExec>("submitToCluster") {
    description = "Submits the LMS job to a running master."
    group = "application"
    mainClass.set("dev.dhruv.streaming.lms.SubmitLmsJob")
    classpath = sourceSets["submit"].runtimeClasspath
}

tasks.register<JavaExec>("publishFixture") {
    description = "Publishes the fixed LMS replay and explicit per-partition progress records."
    group = "application"
    mainClass.set("dev.dhruv.streaming.lms.PublishLmsFixture")
    classpath = sourceSets["submit"].runtimeClasspath
    args(rootProject.file("demos/fixtures").absolutePath)
    if (providers.gradleProperty("fixtureProgressOnly").orNull == "true") {
        args("--progress-only")
    }
}

// Windows limits a process command line to roughly 32 KiB. This process-level test has a broad
// runtime (master, workers, Iceberg, Testcontainers); expanding that graph once for Gradle's test
// JVM and again in processTestClasspath crossed the limit when Phase 7 added metrics. A manifest
// classpath keeps both launches to one short JAR path while preserving the resolved dependency set.
val integrationTestPathingJar by tasks.registering(Jar::class) {
    archiveClassifier.set("integration-test-pathing")
    dependsOn(tasks.named("integrationTestClasses"))
    // The manifest points at dependent project JARs. Build their producers as well as our
    // classes, otherwise a focused test can silently launch an older master/worker binary.
    dependsOn(sourceSets["integrationTest"].runtimeClasspath)
    inputs.files(sourceSets["integrationTest"].runtimeClasspath)
    doFirst {
        manifest.attributes["Class-Path"] = sourceSets["integrationTest"].runtimeClasspath.files
            .joinToString(" ") { it.toURI().toASCIIString() }
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs the process-level checkpoint and worker-recovery acceptance test."
    group = "verification"
    dependsOn(integrationTestPathingJar)
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = files(integrationTestPathingJar.flatMap { it.archiveFile })
    useJUnitPlatform()
    shouldRunAfter(tasks.test)

    // A Gradle test worker's java.class.path is only its bootstrap jar. Give the test the real
    // pathing JAR explicitly so its child JVMs use the same portable, short classpath.
    doFirst {
        systemProperty("processTestClasspath",
            integrationTestPathingJar.get().archiveFile.get().asFile.absolutePath)
    }
}

tasks.named("check") {
    dependsOn("integrationTest")
}
