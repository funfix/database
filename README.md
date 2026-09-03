# Funfix Database

[![Maven Central](https://img.shields.io/maven-central/v/org.funfix/delayedqueue-jvm.svg)](https://search.maven.org/artifact/org.funfix/delayedqueue-jvm)
[![javadoc](https://javadoc.io/badge2/org.funfix/delayedqueue-jvm/javadoc.svg)](https://javadoc.io/doc/org.funfix/delayedqueue-jvm)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Delayed Queue

A delayed, high-performance FIFO queue for the JVM, powered by your favorite RDBMS. 💨

- Schedule messages for future delivery
- Poll with an acknowledgement callback; unacked messages get redelivered after a timeout
- More reliable than many message queuing systems (MQ)
- Batch offers for bulk scheduling
- Cron-like scheduling for periodic tasks

Supported database systems:
- H2
- HSQLDB
- MariaDB
- Microsoft SQL Server
- MySQL
- Oracle
- PostgreSQL
- SQLite

Built on Java 21+, making good use of virtual threads. Has Scala (2.13, 3.x) integration.

### Documentation

- For Java: 
  - [Java Usage Guide](./docs/java-usage-guide.md)
  - [Javadoc](https://javadoc.io/doc/org.funfix/delayedqueue-jvm/0.5.2/)
- For Scala: 
  - [Scala Usage Guide](./docs/scala-usage-guide.md)
  - [Scaladoc](https://javadoc.io/doc/org.funfix/delayedqueue-scala_3/0.5.2/)
- [Internals](./docs/internals.md)

---

Maven:
```xml
<!-- Java or Kotlin -->
<dependency>
  <groupId>org.funfix</groupId>
  <artifactId>delayedqueue-jvm</artifactId>
  <version>0.5.2</version>
</dependency>
<!-- Scala 3 integration-->
<dependency>
  <groupId>org.funfix</groupId>
  <artifactId>delayedqueue-scala_3</artifactId>
  <version>0.5.2</version>
</dependency>
```

Gradle:
```kotlin
dependencies {
    // Java or Kotlin
    implementation("org.funfix:delayedqueue-jvm:0.5.2")
    // Scala 3 integration
    implementation("org.funfix:delayedqueue-scala_3:0.5.2")
}
```

sbt:
```scala
// Base JVM library
libraryDependencies += "org.funfix" %% "delayedqueue-jvm" % "0.5.2"
// Scala integration (2.13 or 3)
libraryDependencies += "org.funfix" %% "delayedqueue-scala" % "0.5.2"
```

You will need to add a supported JDBC driver:
- H2: [com.h2database:h2](https://central.sonatype.com/artifact/com.h2database/h2)
- HSQLDB: [org.hsqldb:hsqldb](https://central.sonatype.com/artifact/org.hsqldb/hsqldb)
- MariaDB: [org.mariadb.jdbc:mariadb-java-client](https://central.sonatype.com/artifact/org.mariadb.jdbc/mariadb-java-client)
- MS-SQL: [com.microsoft.sqlserver:mssql-jdbc](https://central.sonatype.com/artifact/com.microsoft.sqlserver/mssql-jdbc)
- Oracle: [com.oracle.database.jdbc:ojdbc11](https://central.sonatype.com/artifact/com.oracle.database.jdbc/ojdbc11)
- PostgreSQL: [org.postgresql:postgresql](https://central.sonatype.com/artifact/org.postgresql/postgresql)
- SQLite: [org.xerial:sqlite-jdbc](https://central.sonatype.com/artifact/org.xerial/sqlite-jdbc)
