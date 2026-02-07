/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
