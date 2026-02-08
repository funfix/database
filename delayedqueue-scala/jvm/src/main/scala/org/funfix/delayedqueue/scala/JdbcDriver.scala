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
final case class JdbcDriver private (className: String) {

  /** Converts this Scala JdbcDriver to a JVM JdbcDriver. */
  def asJava: jvm.JdbcDriver =
    JdbcDriver.jvmEntries.getOrElse(
      this,
      throw new IllegalArgumentException(s"Unknown JDBC driver: $className")
    )
}

object JdbcDriver {

  val HSQLDB: JdbcDriver = jvm.JdbcDriver.HSQLDB.asScala
  val H2: JdbcDriver = jvm.JdbcDriver.H2.asScala
  val MsSqlServer: JdbcDriver = jvm.JdbcDriver.MsSqlServer.asScala
  val Sqlite: JdbcDriver = jvm.JdbcDriver.Sqlite.asScala
  val MariaDB: JdbcDriver = jvm.JdbcDriver.MariaDB.asScala
  val MySQL: JdbcDriver = jvm.JdbcDriver.MySQL.asScala
  val PostgreSQL: JdbcDriver = jvm.JdbcDriver.PostgreSQL.asScala
  val Oracle: JdbcDriver = jvm.JdbcDriver.Oracle.asScala

  val entries: List[JdbcDriver] =
    List(H2, HSQLDB, MariaDB, MsSqlServer, MySQL, PostgreSQL, Sqlite, Oracle)

  private val jvmEntries: Map[JdbcDriver, jvm.JdbcDriver] = Map(
    H2 -> jvm.JdbcDriver.H2,
    HSQLDB -> jvm.JdbcDriver.HSQLDB,
    MariaDB -> jvm.JdbcDriver.MariaDB,
    MsSqlServer -> jvm.JdbcDriver.MsSqlServer,
    MySQL -> jvm.JdbcDriver.MySQL,
    PostgreSQL -> jvm.JdbcDriver.PostgreSQL,
    Sqlite -> jvm.JdbcDriver.Sqlite,
    Oracle -> jvm.JdbcDriver.Oracle
  )

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
