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
configurations["integrationTestImplementation"]
    .extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"]
    .extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    add("submitImplementation", project(":engine-master"))
    add("integrationTestImplementation", project(":engine-master"))
    add("integrationTestImplementation", project(":engine-worker"))
}

tasks.register<JavaExec>("submitToCluster") {
    description = "Submits the LMS job to a running master."
    group = "application"
    mainClass.set("dev.dhruv.streaming.lms.SubmitLmsJob")
    classpath = sourceSets["submit"].runtimeClasspath
}

tasks.register<Test>("integrationTest") {
    description = "Runs the process-level checkpoint and worker-recovery acceptance test."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)

    // A Gradle test worker's java.class.path is only its bootstrap jar. Give the test the real
    // resolved classpath explicitly so child JVMs can launch portably on Windows and Unix.
    systemProperty("processTestClasspath", sourceSets["integrationTest"].runtimeClasspath.asPath)
}

tasks.named("check") {
    dependsOn("integrationTest")
}
