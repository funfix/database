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

import java.time.Duration
import org.funfix.delayedqueue.jvm

/** Configuration for tuning the Hikari connection pool.
  *
  * @param connectionTimeout
  *   maximum time to wait for a connection from the pool
  * @param idleTimeout
  *   maximum time a connection can sit idle in the pool
  * @param maxLifetime
  *   maximum lifetime of a connection in the pool
  * @param keepaliveTime
  *   frequency of keepalive checks
  * @param maximumPoolSize
  *   maximum number of connections in the pool
  * @param minimumIdle
  *   minimum number of idle connections to maintain
  * @param leakDetectionThreshold
  *   time before a connection is considered leaked
  * @param initializationFailTimeout
  *   time to wait for pool initialization
  */
final case class JdbcDatabasePoolConfig(
    connectionTimeout: Duration = Duration.ofSeconds(30),
    idleTimeout: Duration = Duration.ofMinutes(10),
    maxLifetime: Duration = Duration.ofMinutes(30),
    keepaliveTime: Duration = Duration.ZERO,
    maximumPoolSize: Int = 10,
    minimumIdle: Option[Int] = None,
    leakDetectionThreshold: Option[Duration] = None,
    initializationFailTimeout: Option[Duration] = None
) {

  /** Converts this Scala JdbcDatabasePoolConfig to a JVM JdbcDatabasePoolConfig. */
  def asJava: jvm.JdbcDatabasePoolConfig =
    new jvm.JdbcDatabasePoolConfig(
      connectionTimeout,
      idleTimeout,
      maxLifetime,
      keepaliveTime,
      maximumPoolSize,
      minimumIdle.map(Int.box).getOrElse(null),
      leakDetectionThreshold.getOrElse(null),
      initializationFailTimeout.getOrElse(null)
    )
}

object JdbcDatabasePoolConfig {

  /** Conversion extension for JVM JdbcDatabasePoolConfig. */
  extension (javaConfig: jvm.JdbcDatabasePoolConfig) {

    /** Converts a JVM JdbcDatabasePoolConfig to a Scala JdbcDatabasePoolConfig. */
    def asScala: JdbcDatabasePoolConfig =
      JdbcDatabasePoolConfig(
        connectionTimeout = javaConfig.connectionTimeout,
        idleTimeout = javaConfig.idleTimeout,
        maxLifetime = javaConfig.maxLifetime,
        keepaliveTime = javaConfig.keepaliveTime,
        maximumPoolSize = javaConfig.maximumPoolSize,
        minimumIdle = Option(javaConfig.minimumIdle).map(_.intValue),
        leakDetectionThreshold = Option(javaConfig.leakDetectionThreshold),
        initializationFailTimeout = Option(javaConfig.initializationFailTimeout)
      )
  }
}
