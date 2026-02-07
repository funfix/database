package org.funfix.delayedqueue.jvm

/** JDBC driver configurations. */
public class JdbcDriver private constructor(public val className: String) {
    public companion object {
        @JvmField public val HSQLDB: JdbcDriver = JdbcDriver("org.hsqldb.jdbc.JDBCDriver")

        @JvmField public val H2: JdbcDriver = JdbcDriver("org.h2.Driver")

        @JvmField
        public val MsSqlServer: JdbcDriver =
            JdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver")

        @JvmField public val Sqlite: JdbcDriver = JdbcDriver("org.sqlite.JDBC")

        @JvmField public val MariaDB: JdbcDriver = JdbcDriver("org.mariadb.jdbc.Driver")

        @JvmField public val MySQL: JdbcDriver = JdbcDriver("com.mysql.cj.jdbc.Driver")

        @JvmField public val PostgreSQL: JdbcDriver = JdbcDriver("org.postgresql.Driver")

        @JvmField public val Oracle: JdbcDriver = JdbcDriver("oracle.jdbc.OracleDriver")

        @JvmStatic
        public val entries: List<JdbcDriver> =
            listOf(H2, HSQLDB, MariaDB, MsSqlServer, MySQL, PostgreSQL, Sqlite)

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
