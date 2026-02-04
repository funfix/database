plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.1.0")
    implementation("io.gitlab.arturbosch.detekt:detekt-gradle-plugin:1.23.7")
    implementation("org.jetbrains.dokka:dokka-gradle-plugin:1.9.20")
    implementation("com.vanniktech:gradle-maven-publish-plugin:0.30.0")
}

kotlin {
    jvmToolchain(21)
}
