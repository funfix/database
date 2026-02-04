import com.vanniktech.maven.publish.SonatypeHost

plugins {
    id("com.vanniktech.maven.publish")
}

mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    
    signAllPublications()
    
    pom {
        name.set(project.name)
        description.set("Delayed queue implementation for JVM")
        url.set("https://github.com/funfix/delayedqueue")
        
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        
        developers {
            developer {
                id.set("funfix")
                name.set("Funfix Contributors")
                email.set("contact@funfix.org")
            }
        }
        
        scm {
            url.set("https://github.com/funfix/delayedqueue")
            connection.set("scm:git:git://github.com/funfix/delayedqueue.git")
            developerConnection.set("scm:git:ssh://git@github.com/funfix/delayedqueue.git")
        }
    }
}
