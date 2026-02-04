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
public data class JdbcConnectionConfig(
    val url: String,
    val driver: JdbcDriver,
    val username: String? = null,
    val password: String? = null,
    val pool: JdbcDatabasePoolConfig? = null
)
