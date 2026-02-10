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

/** Configuration for JDBC-based delayed queue instances.
  *
  * This configuration groups together all settings needed to create a
  * DelayedQueueJDBC instance.
  *
  * ==Example==
  *
  * {{{
  * val dbConfig = JdbcConnectionConfig(
  *   url = "jdbc:hsqldb:mem:testdb",
  *   driver = JdbcDriver.HSQLDB,
  *   username = "SA",
  *   password = "",
  *   pool = null
  * )
  *
  * val config = DelayedQueueJDBCConfig(
  *   db = dbConfig,
  *   tableName = "delayed_queue_table",
  *   time = DelayedQueueTimeConfig.DEFAULT_JDBC,
  *   queueName = "my-queue",
  *   ackEnvSource = "DelayedQueueJDBC:my-queue",
  *   retryPolicy = Some(RetryConfig.DEFAULT)
  * )
  * }}}
  *
  * @param db
  *   JDBC connection configuration
  * @param tableName
  *   Name of the database table to use for storing queue messages
  * @param time
  *   Time configuration for queue operations (poll periods, timeouts, etc.)
  * @param queueName
  *   Unique name for this queue instance, used for partitioning messages in
  *   shared tables. Multiple queue instances can share the same database table
  *   if they have different queue names.
  * @param ackEnvSource
  *   Source identifier for acknowledgement envelopes, used for tracing and
  *   debugging. Typically, follows the pattern "DelayedQueueJDBC:{queueName}".
  * @param retryPolicy
  *   Optional retry configuration for database operations. If None, uses
  *   RetryConfig.DEFAULT.
  */
final case class DelayedQueueJDBCConfig(
  db: JdbcConnectionConfig,
  tableName: String,
  time: DelayedQueueTimeConfig,
  queueName: String,
  ackEnvSource: String,
  retryPolicy: Option[RetryConfig]
) {
  require(tableName.nonEmpty, "tableName must not be blank")
  require(queueName.nonEmpty, "queueName must not be blank")
  require(ackEnvSource.nonEmpty, "ackEnvSource must not be blank")

  /** Converts this Scala DelayedQueueJDBCConfig to a JVM
    * DelayedQueueJDBCConfig.
    */
  def asJava: jvm.DelayedQueueJDBCConfig =
    new jvm.DelayedQueueJDBCConfig(
      db.asJava,
      tableName,
      time.asJava,
      queueName,
      ackEnvSource,
      retryPolicy.map(_.asJava).getOrElse(null)
    )
}

object DelayedQueueJDBCConfig {

  /** Creates a default configuration for the given database, table name, and
    * queue name.
    *
    * @param db
    *   JDBC connection configuration
    * @param tableName
    *   Name of the database table to use
    * @param queueName
    *   Unique name for this queue instance
    * @return
    *   A configuration with default time and retry policies
    */
  def create(
    db: JdbcConnectionConfig,
    tableName: String,
    queueName: String
  ): DelayedQueueJDBCConfig =
    DelayedQueueJDBCConfig(
      db = db,
      tableName = tableName,
      time = DelayedQueueTimeConfig.DEFAULT_JDBC,
      queueName = queueName,
      ackEnvSource = s"DelayedQueueJDBC:$queueName",
      retryPolicy = Some(RetryConfig.DEFAULT)
    )

  /** Conversion extension for JVM DelayedQueueJDBCConfig. */
  extension (javaConfig: jvm.DelayedQueueJDBCConfig) {

    /** Converts a JVM DelayedQueueJDBCConfig to a Scala DelayedQueueJDBCConfig.
      */
    def asScala: DelayedQueueJDBCConfig =
      DelayedQueueJDBCConfig(
        db = JdbcConnectionConfig.asScala(javaConfig.db),
        tableName = javaConfig.tableName,
        time = DelayedQueueTimeConfig.asScala(javaConfig.time),
        queueName = javaConfig.queueName,
        ackEnvSource = javaConfig.ackEnvSource,
        retryPolicy = Option(javaConfig.retryPolicy).map(RetryConfig.asScala)
      )
  }
}
