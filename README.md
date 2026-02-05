# Delayed Queue

[![Maven Central](https://img.shields.io/maven-central/v/org.funfix/delayedqueue-jvm.svg)](https://search.maven.org/artifact/org.funfix/delayedqueue-jvm)
[![javadoc](https://javadoc.io/badge2/org.funfix/delayedqueue-jvm/javadoc.svg)](https://javadoc.io/doc/org.funfix/delayedqueue-jvm)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A delayed, FIFO queue for the JVM with Java-first APIs and explicit acknowledgement semantics.

- Schedule messages for future delivery with `Instant` timestamps
- Poll with an acknowledgement callback; unacked messages get redelivered after a timeout
- Batch offers for bulk scheduling
- Cron-like scheduling helpers for periodic or daily ticks


## Usage

Read the [Javadoc](https://javadoc.io/doc/org.funfix/delayedqueue-jvm/0.1.0/org/funfix/tasks/jvm/package-summary.html).
Better documentation is coming.

---

Maven:
```xml
<dependency>
  <groupId>org.funfix</groupId>
  <artifactId>delayedqueue-jvm</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle:
```kotlin
dependencies {
    implementation("org.funfix:delayedqueue-jvm:0.1.0")
}
```

sbt:
```scala
libraryDependencies += "org.funfix" % "delayedqueue-jvm" % "0.1.0"
```
