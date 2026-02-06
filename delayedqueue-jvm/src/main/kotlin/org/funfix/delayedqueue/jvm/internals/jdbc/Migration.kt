package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.Connection

/**
 * Represents a database migration with SQL and a test to check if it needs to run.
 *
 * @property sql The SQL statement(s) to execute for this migration
 * @property needsExecution Function that tests if this migration needs to be executed
 */
internal data class Migration(val sql: String, val needsExecution: (Connection) -> Boolean) {
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

        private fun tableExists(connection: Connection, tableName: String): Boolean {
            val metadata = connection.metaData
            metadata.getTables(null, null, tableName, null).use { rs ->
                return rs.next()
            }
        }

        private fun columnExists(
            connection: Connection,
            tableName: String,
            columnName: String,
        ): Boolean {
            val metadata = connection.metaData
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
     * @param connection The database connection
     * @param migrations List of migrations to run
     * @return Number of migrations executed
     */
    fun runMigrations(connection: Connection, migrations: List<Migration>): Int {
        var executed = 0
        for (migration in migrations) {
            if (migration.needsExecution(connection)) {
                connection.createStatement().use { stmt ->
                    // Split by semicolon to handle multiple statements
                    migration.sql
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
