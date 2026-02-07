package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.utils.Raise

/**
 * Represents a database migration with SQL and a test to check if it needs to run.
 *
 * @property sql The SQL statement(s) to execute for this migration
 * @property needsExecution Function that tests if this migration needs to be executed
 */
internal data class Migration(val sql: String, val needsExecution: (SafeConnection) -> Boolean) {
    companion object {
        /**
         * Creates a migration that checks if a table exists.
         *
         * @param tableName The table name to check for
         * @param sql The SQL to execute if table doesn't exist
         */
        fun createTableIfNotExists(tableName: String, sql: String): Migration =
            Migration(
                sql = sql,
                needsExecution = { connection -> !tableExists(connection, tableName) },
            )

        /**
         * Creates a migration that checks if a column exists in a table.
         *
         * @param tableName The table to check
         * @param columnName The column to look for
         * @param sql The SQL to execute if column doesn't exist
         */
        fun addColumnIfNotExists(tableName: String, columnName: String, sql: String): Migration =
            Migration(
                sql = sql,
                needsExecution = { connection ->
                    tableExists(connection, tableName) &&
                        !columnExists(connection, tableName, columnName)
                },
            )

        /**
         * Creates a migration that always needs to run (e.g., for indexes that are idempotent).
         *
         * @param sql The SQL to execute
         */
        fun alwaysRun(sql: String): Migration = Migration(sql = sql, needsExecution = { _ -> true })

        private fun tableExists(conn: SafeConnection, tableName: String): Boolean {
            val metadata = conn.underlying.metaData
            metadata.getTables(null, null, tableName, null).use { rs ->
                return rs.next()
            }
        }

        private fun columnExists(
            conn: SafeConnection,
            tableName: String,
            columnName: String,
        ): Boolean {
            val metadata = conn.underlying.metaData
            metadata.getColumns(null, null, tableName, columnName).use { rs ->
                return rs.next()
            }
        }
    }
}

/** Executes migrations on a database connection. */
internal object MigrationRunner {
    /**
     * Runs all migrations that need execution.
     *
     * @param conn The database connection
     * @param migrations List of migrations to run
     * @return Number of migrations executed
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun runMigrations(conn: SafeConnection, migrations: List<Migration>): Int {
        var executed = 0
        for (migration in migrations) {
            if (migration.needsExecution(conn)) {
                conn.createStatement { stmt ->
                    // Split by semicolon to handle multiple statements
                    migration.sql
                        .trimIndent()
                        .split(";")
                        .map { it.trim() }
                        .filter { it.isNotEmpty() }
                        .forEach { sql -> stmt.execute(sql) }
                }
                executed++
            }
        }
        return executed
    }
}
