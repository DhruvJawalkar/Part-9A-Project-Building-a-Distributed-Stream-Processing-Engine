dependencies {
    api(project(":engine-api"))
    // Graph types only -- the master compiles a logical graph to a physical one.
    implementation(project(":engine-runtime"))

    implementation(libs.slf4j.api)
}
