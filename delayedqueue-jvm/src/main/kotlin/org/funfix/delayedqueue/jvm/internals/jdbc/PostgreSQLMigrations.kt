package org.funfix.delayedqueue.jvm.internals.jdbc

/** PostgreSQL-specific migrations for the DelayedQueue table. */
internal object PostgreSQLMigrations {
    /**
     * Gets the list of migrations for PostgreSQL.
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
                        id BIGSERIAL NOT NULL,
                        pKey VARCHAR(200) NOT NULL,
                        pKind VARCHAR(100) NOT NULL,
                        payload BYTEA NOT NULL,
                        scheduledAt BIGINT NOT NULL,
                        scheduledAtInitially BIGINT NOT NULL,
                        lockUuid VARCHAR(36) NULL,
                        createdAt BIGINT NOT NULL,
                        PRIMARY KEY (pKey, pKind)
                    );

                    CREATE UNIQUE INDEX ${tableName}__IdUniqueIndex ON $tableName(id);
                    CREATE INDEX ${tableName}__KindPlusScheduledAtIndex ON $tableName(pKind, scheduledAt);
                    CREATE INDEX ${tableName}__LockUuidPlusIdIndex ON $tableName(lockUuid, id)
                    """
                        .trimIndent(),
            )
        )
}
