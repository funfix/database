package org.funfix.delayedqueue.jvm.internals.jdbc

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
            Migration.createTableIfNotExists(
                tableName = tableName,
                sql =
                    """
                    CREATE TABLE $tableName (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pKey TEXT NOT NULL,
                        pKind TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        scheduledAt TEXT NOT NULL,
                        scheduledAtInitially TEXT NOT NULL,
                        lockUuid TEXT NULL,
                        createdAt TEXT NOT NULL
                    );

                    CREATE UNIQUE INDEX ${tableName}__PKindPKeyUniqueIndex
                    ON $tableName (pKind, pKey);

                    CREATE INDEX ${tableName}__ScheduledAtIndex
                    ON $tableName (scheduledAt);

                    CREATE INDEX ${tableName}__KindPlusScheduledAtIndex
                    ON $tableName (pKind, scheduledAt);

                    CREATE INDEX ${tableName}__CreatedAtIndex
                    ON $tableName (createdAt);

                    CREATE INDEX ${tableName}__LockUuidPlusIdIndex
                    ON $tableName (lockUuid, id);
                    """
                        .trimIndent(),
            )
        )
}
