import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jetbrains.dokka.gradle.DokkaExtension
import org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode
import org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation

plugins {
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.dokka")
    id("org.jetbrains.dokka-javadoc")
    id("com.ncorti.ktfmt.gradle")
    id("org.jetbrains.kotlinx.kover")
}

repositories {
    mavenCentral()
}

kotlin {
    explicitApi()

    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
        languageVersion.set(KotlinVersion.KOTLIN_2_3)
        apiVersion.set(KotlinVersion.KOTLIN_2_3)

        allWarningsAsErrors.set(true)
        progressiveMode.set(true)
        jvmDefault.set(JvmDefaultMode.NO_COMPATIBILITY)

        freeCompilerArgs.addAll(
            "-Xjsr305=strict",
            "-Xemit-jvm-type-annotations",
            "-Xcontext-parameters",
        )
    }

    @OptIn(ExperimentalAbiValidation::class)
    abiValidation {
        enabled.set(true)
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21

    withSourcesJar()
    withJavadocJar()
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
    }
}

extensions.configure<DokkaExtension>("dokka") {
    dokkaSourceSets.configureEach {
        jdkVersion.set(21)
        skipEmptyPackages.set(true)
    }
}

tasks.named<Jar>("javadocJar") {
    from(tasks.named("dokkaGeneratePublicationJavadoc"))
}

tasks.matching { it.name == "dokkaJavadocJar" }.configureEach {
    enabled = false
}

tasks.named("check").configure {
    dependsOn(tasks.matching { it.name == "checkLegacyAbi" })
}

ktfmt {
    kotlinLangStyle()
    removeUnusedImports = true
}

kover {
    reports {
        total {
            html {
                onCheck = false
            }
            xml {
                onCheck = false
            }
        }
    }
}
