import java.io.FileInputStream
import java.util.Properties

plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

val props = run {
    val projectProperties = Properties()
    val fis = FileInputStream("$rootDir/../gradle.properties")
    projectProperties.load(fis)
    projectProperties
}

fun version(k: String) =
    props.getProperty("versions.$k")

dependencies {
    implementation(libs.gradle.versions.plugin)
    implementation(libs.vanniktech.publish.plugin)
    implementation(libs.kotlin.gradle.plugin)
    implementation(libs.kover.gradle.plugin)
    implementation(libs.dokka.gradle.plugin)
    implementation(libs.detekt.gradle.plugin)
    implementation(libs.binary.compatibility.validator.plugin)
    implementation(libs.ktfmt.gradle.plugin)
}

kotlin {
    jvmToolchain(21)
}
