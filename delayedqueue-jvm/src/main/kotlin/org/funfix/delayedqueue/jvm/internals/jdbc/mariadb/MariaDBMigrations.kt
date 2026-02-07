package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import org.funfix.delayedqueue.jvm.internals.jdbc.Migration

/**
 * MariaDB-specific migrations for the DelayedQueue table.
 *
 * MariaDB shares the same SQL syntax as MySQL, so we use the shared MySQLCompatibleMigrations.
 */
internal object MariaDBMigrations {
    /**
     * Gets the list of migrations for MariaDB.
     *
     * @param tableName The name of the delayed queue table
     * @return List of migrations in order
     */
    fun getMigrations(tableName: String): List<Migration> =
        MySQLCompatibleMigrations.getMigrations(tableName)
}
