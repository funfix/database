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

package org.funfix.delayedqueue.jvm

/**
 * Configuration for JDBC-based delayed queue instances.
 *
 * This configuration groups together all settings needed to create a [DelayedQueueJDBC] instance.
 *
 * ## Java Usage
 *
 * ```java
 * JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
 *     "jdbc:hsqldb:mem:testdb",
 *     JdbcDriver.HSQLDB,
 *     "SA",
 *     "",
 *     null
 * );
 *
 * DelayedQueueJDBCConfig config = new DelayedQueueJDBCConfig(
 *     dbConfig,                            // db
 *     "delayed_queue_table",               // tableName
 *     DelayedQueueTimeConfig.DEFAULT_JDBC, // time
 *     "my-queue",                          // queueName
 *     "DelayedQueueJDBC:my-queue",         // ackEnvSource
 *     RetryConfig.DEFAULT                  // retryPolicy (optional, can be null)
 * );
 * ```
 *
 * @param db JDBC connection configuration
 * @param tableName Name of the database table to use for storing queue messages
 * @param time Time configuration for queue operations (poll periods, timeouts, etc.)
 * @param queueName Unique name for this queue instance, used for partitioning messages in shared
 *   tables. Multiple queue instances can share the same database table if they have different queue
 *   names.
 * @param ackEnvSource Source identifier for acknowledgement envelopes, used for tracing and
 *   debugging. Typically, follows the pattern "DelayedQueueJDBC:{queueName}".
 * @param retryPolicy Optional retry configuration for database operations. If null, uses
 *   [RetryConfig.DEFAULT].
 */
@JvmRecord
public data class DelayedQueueJDBCConfig
@JvmOverloads
constructor(
    val db: JdbcConnectionConfig,
    val tableName: String,
    val time: DelayedQueueTimeConfig,
    val queueName: String,
    val ackEnvSource: String = "DelayedQueueJDBC:$queueName",
    val retryPolicy: RetryConfig? = null,
) {
    init {
        require(tableName.isNotBlank()) { "tableName must not be blank" }
        require(queueName.isNotBlank()) { "queueName must not be blank" }
        require(ackEnvSource.isNotBlank()) { "ackEnvSource must not be blank" }
    }

    public companion object {
        /**
         * Creates a default configuration for the given database, table name, and queue name.
         *
         * @param db JDBC connection configuration
         * @param tableName Name of the database table to use
         * @param queueName Unique name for this queue instance
         * @return A configuration with default time and retry policies
         */
        @JvmStatic
        public fun create(
            db: JdbcConnectionConfig,
            tableName: String,
            queueName: String,
        ): DelayedQueueJDBCConfig =
            DelayedQueueJDBCConfig(
                db = db,
                tableName = tableName,
                time = DelayedQueueTimeConfig.DEFAULT_JDBC,
                queueName = queueName,
                ackEnvSource = "DelayedQueueJDBC:$queueName",
                retryPolicy = RetryConfig.DEFAULT,
            )
    }
}
