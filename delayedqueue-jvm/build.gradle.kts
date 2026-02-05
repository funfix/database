plugins {
    id("delayedqueue.base")
    id("delayedqueue.publish")
    id("delayedqueue.versions")
}

mavenPublishing {
    pom {
        name.set("Funfix DelayedQueue (JVM)")
        description.set(
            "A delayed, high-performance FIFO queue for the JVM, powered by your favorite RDBMS."
        )
    }
}

dependencies {
    implementation(libs.funfix.tasks.jvm)
    implementation(libs.hikaricp)

    testImplementation(libs.logback.classic)
    testImplementation(libs.jdbc.hsqldb)
    testImplementation(libs.jdbc.sqlite)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test { useJUnitPlatform() }
