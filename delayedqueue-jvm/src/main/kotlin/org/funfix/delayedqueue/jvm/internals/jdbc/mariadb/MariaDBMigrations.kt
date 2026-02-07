package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import org.funfix.delayedqueue.jvm.internals.jdbc.Migration

/** MariaDB-specific migrations for the DelayedQueue table. */
internal object MariaDBMigrations {
    /**
     * Gets the list of migrations for MariaDB.
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

                    CREATE INDEX `${tableName}__KindPlusScheduledAtIndex` ON `$tableName`(`pKind`, `scheduledAt`, `id`);
                    CREATE INDEX `${tableName}__LockUuidPlusIdIndex` ON `$tableName`(`lockUuid`, `id`)
                    """,
            ),
            // Migration 2: Update index to include 'id' column for deterministic ordering
            Migration(
                sql =
                    """
                    DROP INDEX IF EXISTS `${tableName}__KindPlusScheduledAtIndex` ON `$tableName`;
                    CREATE INDEX `${tableName}__KindPlusScheduledAtIndex` ON `$tableName`(`pKind`, `scheduledAt`, `id`);
                    """,
                needsExecution = { conn ->
                    // Check if index exists but doesn't have the 'id' column
                    val metadata = conn.underlying.metaData
                    var hasIndex = false
                    var hasIdColumn = false
                    
                    metadata.getIndexInfo(null, null, tableName, false, false).use { rs ->
                        while (rs.next()) {
                            val indexName = rs.getString("INDEX_NAME")
                            if (indexName == "${tableName}__KindPlusScheduledAtIndex") {
                                hasIndex = true
                                val columnName = rs.getString("COLUMN_NAME")
                                val ordinalPosition = rs.getShort("ORDINAL_POSITION")
                                // Check if 'id' is in position 3
                                if (columnName == "id" && ordinalPosition == 3.toShort()) {
                                    hasIdColumn = true
                                }
                            }
                        }
                    }
                    
                    // Run migration if index exists but doesn't have id column
                    hasIndex && !hasIdColumn
                },
            ),
        )
}
