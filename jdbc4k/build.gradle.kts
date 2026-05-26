plugins {
    id("delayedqueue.base")
    id("delayedqueue.publish")
    id("delayedqueue.versions")
}

mavenPublishing {
    pom {
        name.set("Funfix JDBC4K")
        description.set(
            "Kotlin utilities for working with JDBC, designed for internal use by Funfix projects."
        )
    }
}

dependencies {
    implementation(libs.kotlin.stdlib)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test { useJUnitPlatform() }
