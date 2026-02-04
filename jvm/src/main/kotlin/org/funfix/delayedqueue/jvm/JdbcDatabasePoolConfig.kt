package org.funfix.delayedqueue.jvm

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Configuration for tuning the Hikari connection pool.
 *
 * @property connectionTimeout maximum time to wait for a connection from the pool
 * @property idleTimeout maximum time a connection can sit idle in the pool
 * @property maxLifetime maximum lifetime of a connection in the pool
 * @property keepaliveTime frequency of keepalive checks
 * @property maximumPoolSize maximum number of connections in the pool
 * @property minimumIdle minimum number of idle connections to maintain
 * @property leakDetectionThreshold time before a connection is considered leaked
 * @property initializationFailTimeout time to wait for pool initialization
 */
@JvmRecord
public data class JdbcDatabasePoolConfig(
    val connectionTimeout: Duration = 30.seconds,
    val idleTimeout: Duration = 10.minutes,
    val maxLifetime: Duration = 30.minutes,
    val keepaliveTime: Duration = 0.milliseconds,
    val maximumPoolSize: Int = 10,
    val minimumIdle: Int? = null,
    val leakDetectionThreshold: Duration? = null,
    val initializationFailTimeout: Duration? = null
)
