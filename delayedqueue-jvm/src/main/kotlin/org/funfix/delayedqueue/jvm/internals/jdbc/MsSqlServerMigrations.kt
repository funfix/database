package org.funfix.delayedqueue.jvm.internals.jdbc

/**
 * MS-SQL Server-specific migrations for the DelayedQueue table.
 *
 * Uses MS-SQL data types:
 * - `NVARCHAR` for Unicode string columns
 * - `DATETIMEOFFSET` for timezone-aware timestamps
 * - `BIGINT IDENTITY(1,1)` for auto-increment
 * - Composite primary key on `(pKey, pKind)` with a separate unique index on `id`
 */
internal object MsSqlServerMigrations {
    /**
     * Gets the list of migrations for MS-SQL Server.
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
                    CREATE TABLE $tableName (
                        id BIGINT IDENTITY(1,1) NOT NULL,
                        pKey NVARCHAR(200) NOT NULL,
                        pKind NVARCHAR(100) NOT NULL,
                        payload NVARCHAR(MAX) NOT NULL,
                        scheduledAt DATETIMEOFFSET NOT NULL,
                        scheduledAtInitially DATETIMEOFFSET NOT NULL,
                        lockUuid VARCHAR(36) NULL,
                        createdAt DATETIMEOFFSET NOT NULL,
                        PRIMARY KEY (pKey, pKind)
                    );

                    CREATE UNIQUE INDEX ${tableName}__IdUniqueIndex
                    ON $tableName (id);

                    CREATE INDEX ${tableName}__ScheduledAtIndex
                    ON $tableName (scheduledAt);

                    CREATE INDEX ${tableName}__KindPlusScheduledAtIndex
                    ON $tableName (pKind, scheduledAt);

                    CREATE INDEX ${tableName}__CreatedAtIndex
                    ON $tableName (createdAt);

                    CREATE INDEX ${tableName}__LockUuidPlusIdIndex
                    ON $tableName (lockUuid, id)
                    """
                        .trimIndent(),
            )
        )
}
