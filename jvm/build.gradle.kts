plugins {
    id("delayedqueue.base")
    id("delayedqueue.publish")
    id("delayedqueue.versions")
}

dependencies {
    implementation(libs.funfix.tasks.jvm)
    implementation(libs.hikaricp)

    testImplementation(libs.logback.classic)
    testImplementation(libs.jdbc.sqlite)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test { useJUnitPlatform() }
