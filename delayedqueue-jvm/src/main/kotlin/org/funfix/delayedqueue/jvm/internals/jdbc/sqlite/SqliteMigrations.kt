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
                    ON $tableName (pKind, scheduledAt);

                    CREATE INDEX ${tableName}__LockUuidPlusIdIndex
                    ON $tableName (lockUuid, id);
                    """,
            )
        )
}
