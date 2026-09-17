plugins {
    application
}

dependencies {
    api(project(":engine-api"))
    // The control-plane contracts: the master serves MasterService and calls WorkerService.
    api(project(":engine-rpc"))
    // Durable job state, and the worker registry it watches.
    api(project(":engine-metadata"))
    // Graph types only -- the master compiles a logical graph to a physical one.
    implementation(project(":engine-runtime"))

    implementation(libs.slf4j.api)

    testImplementation(libs.logback)
    runtimeOnly(libs.logback)
}

application {
    mainClass.set("dev.dhruv.streaming.master.MasterBootstrap")
}
