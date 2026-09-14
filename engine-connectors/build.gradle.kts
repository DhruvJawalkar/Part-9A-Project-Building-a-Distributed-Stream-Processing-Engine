dependencies {
    api(project(":engine-api"))
    implementation(project(":engine-runtime"))

    implementation(libs.kafka.clients)
    implementation(libs.jackson)
    implementation(libs.slf4j.api)
}
