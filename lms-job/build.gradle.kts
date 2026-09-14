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

    // NOTE what is absent: engine-runtime. The discipline that actually matters -- the job
    // must never reach into the engine's internals -- is still enforced mechanically, because
    // engine-connectors declares engine-runtime as `implementation` rather than `api`, so it
    // does not leak onto this module's compile classpath. If a job class ever fails to compile
    // because it cannot see a runtime type, that is the rule working, not a build problem.
    runtimeOnly(project(":engine-runtime"))
    runtimeOnly(libs.logback)
}

application {
    mainClass.set("dev.dhruv.streaming.lms.LmsClickstreamJob")
}
