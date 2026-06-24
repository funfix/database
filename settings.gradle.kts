rootProject.name = "delayedqueue"

include("delayedqueue-jvm")
include("jdbc4k")

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
