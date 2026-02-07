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
 * Represents the configuration for a JDBC connection.
 *
 * @property url the JDBC connection URL
 * @property driver the JDBC driver to use
 * @property username optional username for authentication
 * @property password optional password for authentication
 * @property pool optional connection pool configuration
 */
@JvmRecord
public data class JdbcConnectionConfig
@JvmOverloads
constructor(
    val url: String,
    val driver: JdbcDriver,
    val username: String? = null,
    val password: String? = null,
    val pool: JdbcDatabasePoolConfig? = null,
)
