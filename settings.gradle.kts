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
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}
