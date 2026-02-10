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

class JdbcDriverSpec extends munit.FunSuite {
  test("JdbcDriver constants should have correct class names") {
    assertEquals(JdbcDriver.HSQLDB.className, "org.hsqldb.jdbc.JDBCDriver")
    assertEquals(JdbcDriver.H2.className, "org.h2.Driver")
    assertEquals(JdbcDriver.PostgreSQL.className, "org.postgresql.Driver")
    assertEquals(JdbcDriver.MySQL.className, "com.mysql.cj.jdbc.Driver")
    assertEquals(JdbcDriver.MariaDB.className, "org.mariadb.jdbc.Driver")
    assertEquals(JdbcDriver.Sqlite.className, "org.sqlite.JDBC")
    assertEquals(JdbcDriver.MsSqlServer.className, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    assertEquals(JdbcDriver.Oracle.className, "oracle.jdbc.OracleDriver")
  }

  test("JdbcDriver entries should contain all drivers") {
    val allDrivers = Set(
      JdbcDriver.HSQLDB,
      JdbcDriver.H2,
      JdbcDriver.PostgreSQL,
      JdbcDriver.MySQL,
      JdbcDriver.MariaDB,
      JdbcDriver.Sqlite,
      JdbcDriver.MsSqlServer,
      JdbcDriver.Oracle
    )

    assertEquals(JdbcDriver.entries.toSet, allDrivers)
  }

  test("asJava and fromJava should be symmetric") {
    JdbcDriver.entries.foreach { driver =>
      val roundTripped = JdbcDriver.fromJava(driver.asJava)
      assertEquals(roundTripped.className, driver.className)
    }
  }
}
