plugins {
    alias(libs.plugins.protobuf)
}

// The wire contracts, and nothing else.
//
// Every message and service definition in src/main/proto, plus the stubs protoc generates from
// them. No hand-written Java lives here at all -- if a type in this module needs behaviour, the
// behaviour belongs in whichever module owns that side of the conversation.
//
// This module exists because the control plane has two ends. engine-master speaks
// MasterService and calls WorkerService; engine-worker does the reverse. Both need the same
// generated types, and neither should depend on the other. Giving the contracts their own
// module is what lets the dependency arrows point inward from both sides.
//
// Dependencies are `api` rather than `implementation`: anything building a gRPC server or
// channel names io.grpc types directly, so they belong on the compile classpath of dependents.
dependencies {
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.grpc.netty.shaded)
    api(libs.protobuf.java)

    // javax.annotation.Generated, referenced by the code protoc-gen-grpc-java emits.
    compileOnly(libs.annotations.api)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

// Generated sources are not ours to lint. Silencing them here means any warning left in the
// build output is about code we actually wrote and can actually fix.
tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.removeIf { it.startsWith("-Xlint") }
    options.compilerArgs.add("-Xlint:none")
}
