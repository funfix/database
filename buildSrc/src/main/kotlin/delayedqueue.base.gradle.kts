import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jetbrains.dokka.gradle.DokkaExtension
import org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation

plugins {
    id("org.jetbrains.kotlin.jvm")
    id("io.gitlab.arturbosch.detekt")
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

        // Temporarily disabled due to Kotlin compiler deprecation warning for -Xjvm-default
        // allWarningsAsErrors.set(true)
        progressiveMode.set(true)

        freeCompilerArgs.addAll(
            "-Xjvm-default=all",
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

detekt {
    buildUponDefaultConfig = true
    allRules = false
    config.setFrom("$rootDir/detekt.yml")
}

tasks.withType<io.gitlab.arturbosch.detekt.Detekt>().configureEach {
    jvmTarget = "21"
    reports {
        html.required.set(true)
        xml.required.set(false)
        txt.required.set(false)
    }
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
