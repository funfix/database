package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import org.funfix.delayedqueue.jvm.internals.jdbc.Migration

/**
 * Migrations for MySQL-compatible databases (MySQL and MariaDB).
 *
 * Both MySQL and MariaDB share the same SQL syntax for table creation, so they can use the same
 * migrations.
 */
internal object MySQLCompatibleMigrations {
    /**
     * Gets the list of migrations for MySQL-compatible databases.
     *
     * @param tableName The name of the delayed queue table
     * @return List of migrations in order
     */
    fun getMigrations(tableName: String): List<Migration> =
        listOf(
            Migration.createTableIfNotExists(
                tableName = tableName,
                sql =
                    """
                    CREATE TABLE `${tableName}` (
                        `id` BIGINT NOT NULL AUTO_INCREMENT,
                        `pKey` VARCHAR(200) NOT NULL,
                        `pKind` VARCHAR(100) NOT NULL,
                        `payload` LONGBLOB NOT NULL,
                        `scheduledAt` BIGINT NOT NULL,
                        `scheduledAtInitially` BIGINT NOT NULL,
                        `lockUuid` VARCHAR(36) NULL,
                        `createdAt` BIGINT NOT NULL,
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `${tableName}__KindPlusKeyUniqueIndex` (`pKind`, `pKey`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

                    CREATE INDEX `${tableName}__KindPlusScheduledAtIndex` ON `$tableName`(`pKind`, `scheduledAt`);
                    CREATE INDEX `${tableName}__LockUuidPlusIdIndex` ON `$tableName`(`lockUuid`, `id`)
                    """,
            )
        )
}
