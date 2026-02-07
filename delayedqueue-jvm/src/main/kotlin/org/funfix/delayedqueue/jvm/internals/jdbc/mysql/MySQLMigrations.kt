package org.funfix.delayedqueue.jvm.internals.jdbc.mysql

import org.funfix.delayedqueue.jvm.internals.jdbc.Migration
import org.funfix.delayedqueue.jvm.internals.jdbc.mariadb.MySQLCompatibleMigrations

/**
 * MySQL-specific migrations for the DelayedQueue table.
 *
 * MySQL shares the same SQL syntax as MariaDB, so we use the shared MySQLCompatibleMigrations.
 */
internal object MySQLMigrations {
    /**
     * Gets the list of migrations for MySQL.
     *
     * @param tableName The name of the delayed queue table
     * @return List of migrations in order
     */
    fun getMigrations(tableName: String): List<Migration> =
        MySQLCompatibleMigrations.getMigrations(tableName)
}
