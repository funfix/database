import org.gradle.api.publish.tasks.GenerateModuleMetadata

plugins {
    id("com.vanniktech.maven.publish")
}

repositories {
    mavenCentral()
}

group = "org.funfix"

val projectVersion = property("project.version").toString()
version = projectVersion.let { version ->
    if (!project.hasProperty("buildRelease"))
        "$version-SNAPSHOT"
    else
        version
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    pom {
        inceptionYear.set("2026")
        url = "https://github.com/funfix/database"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
            }
        }

        developers {
            developer {
                id = "alexandru"
                name = "Alexandru Nedelcu"
                email = "noreply@alexn.org"
            }
        }

        scm {
            connection = "scm:git:git://github.com/funfix/database.git"
            developerConnection = "scm:git:ssh://github.com/funfix/database.git"
            url = "https://github.com/funfix/database"
        }

        issueManagement {
            system = "GitHub"
            url = "https://github.com/funfix/database/issues"
        }
    }
}

tasks.register("printInfo") {
    doLast {
        println("Group: $group")
        println("Project version: $version")
    }
}

tasks.withType<GenerateModuleMetadata>().configureEach {
    dependsOn(tasks.matching { it.name == "dokkaJavadocJar" })
}
