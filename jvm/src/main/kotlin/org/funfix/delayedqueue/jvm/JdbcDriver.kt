package org.funfix.delayedqueue.jvm

/**
 * JDBC driver configurations.
 */
public sealed interface JdbcDriver {
    /**
     * The JDBC driver class name.
     */
    public val className: String

    /**
     * Microsoft SQL Server driver.
     */
    public data object MsSqlServer : JdbcDriver {
        override val className: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    /**
     * SQLite driver.
     */
    public data object Sqlite : JdbcDriver {
        override val className: String = "org.sqlite.JDBC"
    }

    /**
     * Factory methods for creating [JdbcDriver] instances.
     */
    public companion object {
        /**
         * Attempt to find a [JdbcDriver] by its class name.
         *
         * @param className the JDBC driver class name
         * @return the [JdbcDriver] if found, null otherwise
         */
        public fun of(className: String): JdbcDriver? = when {
            className.equals(MsSqlServer.className, ignoreCase = true) -> MsSqlServer
            className.equals(Sqlite.className, ignoreCase = true) -> Sqlite
            else -> null
        }
    }
}
