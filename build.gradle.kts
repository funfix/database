plugins {
    id("java")
}

group = "org.funfix"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.jetbrains:annotations:24.0.0")
    compileOnly("org.jspecify:jspecify:1.0.0")

    implementation("org.funfix:tasks-jvm:0.3.1")
    implementation("com.zaxxer:HikariCP:6.3.2")

    testImplementation("ch.qos.logback:logback-classic:1.5.18")
    testImplementation("org.xerial:sqlite-jdbc:3.50.3.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}