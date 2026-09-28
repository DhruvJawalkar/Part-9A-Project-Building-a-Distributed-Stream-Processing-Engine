dependencies {
    api(project(":engine-api"))

    // Serialization and the transport client live here, so the runtime needs the wire types.
    api(project(":engine-rpc"))

    implementation(libs.slf4j.api)
    implementation(libs.rocksdb.jni)
}
