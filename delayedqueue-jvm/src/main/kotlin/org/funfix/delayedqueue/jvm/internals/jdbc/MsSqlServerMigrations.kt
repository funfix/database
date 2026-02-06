package org.funfix.delayedqueue.jvm.internals.jdbc

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
                    CREATE TABLE $tableName (
                        id BIGINT IDENTITY(1,1) NOT NULL,
                        pKey NVARCHAR(200) NOT NULL,
                        pKind NVARCHAR(100) NOT NULL,
                        payload VARBINARY(MAX) NOT NULL,
                        scheduledAt BIGINT NOT NULL,
                        scheduledAtInitially BIGINT NOT NULL,
                        lockUuid VARCHAR(36) NULL,
                        createdAt BIGINT NOT NULL
                    );

                    ALTER TABLE $tableName ADD PRIMARY KEY (pKey, pKind);

                    CREATE UNIQUE INDEX ${tableName}__IdUniqueIndex ON $tableName(id);
                    CREATE INDEX ${tableName}__KindPlusScheduledAtIndex ON $tableName(pKind, scheduledAt);
                    CREATE INDEX ${tableName}__LockUuidPlusIdIndex ON $tableName(lockUuid, id);
                    """
                        .trimIndent(),
            )
        )
}
