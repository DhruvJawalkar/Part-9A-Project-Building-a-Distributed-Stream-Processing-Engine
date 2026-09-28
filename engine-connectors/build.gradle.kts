dependencies {
    api(project(":engine-api"))
    implementation(project(":engine-runtime"))

    implementation(libs.kafka.clients)
    implementation(libs.jackson)
    implementation(libs.slf4j.api)
    implementation(libs.iceberg.core)
    implementation(libs.iceberg.parquet)
    implementation(libs.iceberg.aws)
    // iceberg-aws intentionally declares the AWS SDK as compile-only. Ship Iceberg's shaded
    // provider bundle so S3FileIO works in worker JVMs without leaking a large SDK surface into
    // connector source compilation.
    runtimeOnly(libs.iceberg.aws.bundle)
    implementation(libs.hadoop.common)
    implementation(libs.parquet.avro)

    // Parquet's generic reader used by the Phase 6 row-level acceptance proof loads Hadoop's
    // FileInputFormat. Workers only write through Iceberg, so keep this larger dependency in tests.
    testImplementation(libs.hadoop.mapreduce.client.core)
}

tasks.test {
    // Opt-in live REST/MinIO smoke test; ordinary unit/acceptance runs remain self-contained.
    systemProperty("icebergRestSmoke", System.getProperty("icebergRestSmoke", "false"))
}
