// Root build. Holds only conventions shared by every module; each module's own
// build file then contains nothing but its dependencies, so the dependency
// direction described in CLAUDE.md section 3 is readable at a glance.

plugins {
    `java-library`
}

allprojects {
    group = "dev.dhruv.streaming"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    apply(plugin = "java-library")

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        // Java 21: sealed interfaces, records and pattern-matching switch are used
        // throughout the engine. A toolchain rather than sourceCompatibility so the
        // build is reproducible regardless of which JDK launches Gradle.
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-serial", "-Xlint:-this-escape"))
    }

    // The test stack is applied here rather than in each module so that
    // engine-api/build.gradle.kts can stay literally free of a dependencies block --
    // its "zero dependencies" rule is about what the API drags onto a user's
    // classpath, and JUnit is not on it.
    dependencies {
        val libs = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")
        add("testImplementation", platform(libs.findLibrary("junit-bom").get()))
        add("testImplementation", libs.findLibrary("junit-jupiter").get())
        add("testImplementation", libs.findLibrary("assertj").get())
        add("testRuntimeOnly", libs.findLibrary("junit-launcher").get())
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }
}
