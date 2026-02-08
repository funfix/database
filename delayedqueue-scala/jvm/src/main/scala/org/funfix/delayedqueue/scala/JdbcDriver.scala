/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.scala

import org.funfix.delayedqueue.jvm

/** JDBC driver configurations.
  *
  * @param className
  *   the JDBC driver class name
  */
final case class JdbcDriver(className: String) {

  /** Converts this Scala JdbcDriver to a JVM JdbcDriver. */
  def asJava: jvm.JdbcDriver = {
    // Find the corresponding JVM driver by class name
    import scala.jdk.CollectionConverters.*
    val jvmEntries = jvm.JdbcDriver.getEntries.asScala
    jvmEntries
      .find(d => d.getClassName() == className)
      .getOrElse {
        throw new IllegalArgumentException(s"Unknown JDBC driver class name: $className")
      }
  }
}

object JdbcDriver {
  val HSQLDB: JdbcDriver = JdbcDriver("org.hsqldb.jdbc.JDBCDriver")
  val H2: JdbcDriver = JdbcDriver("org.h2.Driver")
  val MsSqlServer: JdbcDriver = JdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver")
  val Sqlite: JdbcDriver = JdbcDriver("org.sqlite.JDBC")
  val MariaDB: JdbcDriver = JdbcDriver("org.mariadb.jdbc.Driver")
  val MySQL: JdbcDriver = JdbcDriver("com.mysql.cj.jdbc.Driver")
  val PostgreSQL: JdbcDriver = JdbcDriver("org.postgresql.Driver")
  val Oracle: JdbcDriver = JdbcDriver("oracle.jdbc.OracleDriver")

  val entries: List[JdbcDriver] =
    List(H2, HSQLDB, MariaDB, MsSqlServer, MySQL, PostgreSQL, Sqlite, Oracle)

  /** Attempt to find a JdbcDriver by its class name.
    *
    * @param className
    *   the JDBC driver class name
    * @return
    *   the JdbcDriver if found, None otherwise
    */
  def fromClassName(className: String): Option[JdbcDriver] =
    entries.find(_.className.equalsIgnoreCase(className))

  /** Conversion extension for JVM JdbcDriver. */
  extension (javaDriver: jvm.JdbcDriver) {

    /** Converts a JVM JdbcDriver to a Scala JdbcDriver. */
    def asScala: JdbcDriver =
      JdbcDriver(javaDriver.getClassName())
  }
}
