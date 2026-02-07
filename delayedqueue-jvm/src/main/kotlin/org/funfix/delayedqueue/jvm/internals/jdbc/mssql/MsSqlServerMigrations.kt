package org.funfix.delayedqueue.jvm.internals.jdbc.mssql

import org.funfix.delayedqueue.jvm.internals.jdbc.Migration

/** MS-SQL-specific migrations for the DelayedQueue table. */
internal object MsSqlServerMigrations {
    /**
     * Gets the list of migrations for MS-SQL.
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
                    CREATE TABLE [$tableName] (
                        [id] BIGINT IDENTITY(1,1) NOT NULL,
                        [pKey] NVARCHAR(200) NOT NULL,
                        [pKind] NVARCHAR(100) NOT NULL,
                        [payload] VARBINARY(MAX) NOT NULL,
                        [scheduledAt] BIGINT NOT NULL,
                        [scheduledAtInitially] BIGINT NOT NULL,
                        [lockUuid] VARCHAR(36) NULL,
                        [createdAt] BIGINT NOT NULL
                    );

                    ALTER TABLE [$tableName] ADD PRIMARY KEY ([id]);
                    CREATE UNIQUE INDEX [${tableName}__PKindPKeyUniqueIndex]
                    ON [$tableName] ([pKind], [pKey]);
                    
                    CREATE INDEX [${tableName}__KindPlusScheduledAtIndex] 
                    ON [$tableName]([pKind], [scheduledAt], [id]);
                    
                    CREATE INDEX [${tableName}__LockUuidPlusIdIndex] 
                    ON [$tableName]([lockUuid], [id]);
                    """,
            ),
            // Migration 2: Update index to include 'id' column for deterministic ordering
            Migration(
                sql =
                    """
                    IF EXISTS (SELECT * FROM sys.indexes WHERE name = '${tableName}__KindPlusScheduledAtIndex')
                    DROP INDEX [${tableName}__KindPlusScheduledAtIndex] ON [$tableName];
                    CREATE INDEX [${tableName}__KindPlusScheduledAtIndex] 
                    ON [$tableName]([pKind], [scheduledAt], [id]);
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
