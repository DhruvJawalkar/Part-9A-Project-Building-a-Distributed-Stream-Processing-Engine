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
    implementation(libs.minio)

    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback)

    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit)
}

sourceSets {
    create("integrationTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }
}

configurations["integrationTestImplementation"]
    .extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"]
    .extendsFrom(configurations.testRuntimeOnly.get())

tasks.register<Test>("integrationTest") {
    description = "Runs MinIO checkpoint-storage integration tests in Testcontainers."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}

tasks.named("check") {
    dependsOn("integrationTest")
}

application {
    mainClass.set("dev.dhruv.streaming.worker.WorkerBootstrap")
}
