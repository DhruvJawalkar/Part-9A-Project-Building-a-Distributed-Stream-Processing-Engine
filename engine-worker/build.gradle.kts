plugins {
    application
}

dependencies {
    api(project(":engine-api"))
    // The control-plane contracts: the worker serves WorkerService and calls MasterService.
    api(project(":engine-rpc"))
    // Its own registration in etcd, under a lease that expires if this process dies.
    api(project(":engine-metadata"))
    implementation(project(":engine-runtime"))

    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback)
}

application {
    mainClass.set("dev.dhruv.streaming.worker.WorkerBootstrap")
}
