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

import java.time.Duration

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
public data class JdbcDatabasePoolConfig
@JvmOverloads
constructor(
    val connectionTimeout: Duration = Duration.ofSeconds(30),
    val idleTimeout: Duration = Duration.ofMinutes(10),
    val maxLifetime: Duration = Duration.ofMinutes(30),
    val keepaliveTime: Duration = Duration.ZERO,
    val maximumPoolSize: Int = 10,
    val minimumIdle: Int? = null,
    val leakDetectionThreshold: Duration? = null,
    val initializationFailTimeout: Duration? = null,
)
