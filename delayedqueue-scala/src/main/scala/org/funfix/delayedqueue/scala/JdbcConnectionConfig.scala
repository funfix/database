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

/** Represents the configuration for a JDBC connection.
  *
  * @param url
  *   the JDBC connection URL
  * @param driver
  *   the JDBC driver to use
  * @param username
  *   optional username for authentication
  * @param password
  *   optional password for authentication
  * @param pool
  *   optional connection pool configuration
  */
final case class JdbcConnectionConfig(
  url: String,
  driver: JdbcDriver,
  username: Option[String] = None,
  password: Option[String] = None,
  pool: Option[JdbcDatabasePoolConfig] = None
) {

  /** Converts this Scala JdbcConnectionConfig to a JVM JdbcConnectionConfig. */
  def asJava: jvm.JdbcConnectionConfig =
    new jvm.JdbcConnectionConfig(
      url,
      driver.asJava,
      username.getOrElse(null),
      password.getOrElse(null),
      pool.map(_.asJava).getOrElse(null)
    )
}

object JdbcConnectionConfig {

  /** Conversion extension for JVM JdbcConnectionConfig. */
  extension (javaConfig: jvm.JdbcConnectionConfig) {

    /** Converts a JVM JdbcConnectionConfig to a Scala JdbcConnectionConfig. */
    def asScala: JdbcConnectionConfig =
      JdbcConnectionConfig(
        url = javaConfig.url,
        driver = JdbcDriver.asScala(javaConfig.driver),
        username = Option(javaConfig.username),
        password = Option(javaConfig.password),
        pool = Option(javaConfig.pool).map(JdbcDatabasePoolConfig.asScala)
      )
  }
}
