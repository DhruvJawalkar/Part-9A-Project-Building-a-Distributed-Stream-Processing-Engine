// Durable state shared by both ends of the control plane.
//
// The same reasoning as engine-rpc, one layer down. The master writes job graphs, assignments
// and state transitions and reads the worker registry; each worker writes its own registration
// under a lease. Both need these types, and neither should depend on the other.
//
// Depends on no engine module, so it points inward from both sides and leaves the dependency
// direction intact.
dependencies {
    implementation(libs.jetcd)
    implementation(libs.slf4j.api)

    // jetcd exposes io.grpc types on its own API (the lease keep-alive takes a StreamObserver),
    // so anything implementing MetadataStore sees them.
    api(libs.grpc.stub)
}

dependencies {
    testImplementation(libs.logback)
    // The integration suite starts the real etcd it exercises. Keeping this dependency scoped
    // here means normal unit tests neither need Docker nor pull a container image.
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit)
}

// Integration tests need a live etcd. Kept out of `test` so that `./gradlew test` stays fast
// and fails only for reasons the assertions are about.
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
    description = "Runs etcd integration tests in a Testcontainers-managed container."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}
