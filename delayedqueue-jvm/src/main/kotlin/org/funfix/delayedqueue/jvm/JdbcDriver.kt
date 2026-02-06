package org.funfix.delayedqueue.jvm

/** JDBC driver configurations. */
public enum class JdbcDriver(public val className: String) {
    /** HSQLDB (HyperSQL Database) driver. */
    HSQLDB("org.hsqldb.jdbc.JDBCDriver"),

    /** Microsoft SQL Server driver. */
    MsSqlServer("com.microsoft.sqlserver.jdbc.SQLServerDriver"),

    /** SQLite driver. */
    Sqlite("org.sqlite.JDBC"),

    /** PostgreSQL driver. */
    PostgreSQL("org.postgresql.Driver");

    public companion object {
        /**
         * Attempt to find a [JdbcDriver] by its class name.
         *
         * @param className the JDBC driver class name
         * @return the [JdbcDriver] if found, null otherwise
         */
        @JvmStatic
        public operator fun invoke(className: String): JdbcDriver? =
            entries.firstOrNull { it.className.equals(className, ignoreCase = true) }
    }
}
