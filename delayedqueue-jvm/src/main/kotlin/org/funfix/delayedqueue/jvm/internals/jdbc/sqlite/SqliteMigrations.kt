package org.funfix.delayedqueue.jvm.internals.jdbc.sqlite

import org.funfix.delayedqueue.jvm.internals.jdbc.Migration

/** SQLite-specific migrations for the DelayedQueue table. */
internal object SqliteMigrations {
    /**
     * Gets the list of migrations for SQLite.
     *
     * @param tableName The name of the delayed queue table
     * @return List of migrations in order
     */
    fun getMigrations(tableName: String): List<Migration> =
        listOf(
            Migration.Companion.createTableIfNotExists(
                tableName = tableName,
                sql =
                    """
                    CREATE TABLE $tableName (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pKey TEXT NOT NULL,
                        pKind TEXT NOT NULL,
                        payload BLOB NOT NULL,
                        scheduledAt INTEGER NOT NULL,
                        scheduledAtInitially INTEGER NOT NULL,
                        lockUuid TEXT NULL,
                        createdAt INTEGER NOT NULL
                    );

                    CREATE UNIQUE INDEX ${tableName}__PKindPKeyUniqueIndex
                    ON $tableName (pKind, pKey);

                    CREATE INDEX ${tableName}__KindPlusScheduledAtIndex
                    ON $tableName (pKind, scheduledAt, id);

                    CREATE INDEX ${tableName}__LockUuidPlusIdIndex
                    ON $tableName (lockUuid, id);
                    """,
            ),
            // Migration 2: Update index to include 'id' column for deterministic ordering
            Migration(
                sql =
                    """
                    DROP INDEX IF EXISTS ${tableName}__KindPlusScheduledAtIndex;
                    CREATE INDEX ${tableName}__KindPlusScheduledAtIndex
                    ON $tableName (pKind, scheduledAt, id);
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
