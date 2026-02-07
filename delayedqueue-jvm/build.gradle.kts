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
    testImplementation(libs.jdbc.mssql)
    testImplementation(libs.jdbc.postgresql)
    testImplementation(libs.jdbc.mariadb)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.mssqlserver)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.mariadb)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test { useJUnitPlatform() }
