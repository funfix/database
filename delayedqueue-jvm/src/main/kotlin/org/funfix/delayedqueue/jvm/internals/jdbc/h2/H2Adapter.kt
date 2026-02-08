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

package org.funfix.delayedqueue.jvm.internals.jdbc.h2

import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.SafeConnection
import org.funfix.delayedqueue.jvm.internals.jdbc.prepareStatement
import org.funfix.delayedqueue.jvm.internals.jdbc.toDBTableRowWithId

/** H2-specific adapter. */
internal class H2Adapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

    override fun selectForUpdateOneRow(
        conn: SafeConnection,
        kind: String,
        key: String,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT
                "id",
                "pKey",
                "pKind",
                "payload",
                "scheduledAt",
                "scheduledAtInitially",
                "lockUuid",
                "createdAt"
            FROM "$tableName"
            WHERE "pKey" = ? AND "pKind" = ?
            LIMIT 1
            FOR UPDATE
            """

        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, key)
            stmt.setString(2, kind)
            stmt.executeQuery().use { rs ->
                if (rs.next()) {
                    rs.toDBTableRowWithId()
                } else {
                    null
                }
            }
        }
    }

    override fun insertOneRow(conn: SafeConnection, row: DBTableRow): Boolean {
        val sql =
            """
            INSERT INTO "$tableName"
            (
                "pKey",
                "pKind",
                "payload",
                "scheduledAt",
                "scheduledAtInitially",
                "createdAt"
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """
        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, row.pKey)
            stmt.setString(2, row.pKind)
            stmt.setBytes(3, row.payload)
            stmt.setEpochMillis(4, row.scheduledAt)
            stmt.setEpochMillis(5, row.scheduledAtInitially)
            stmt.setEpochMillis(6, row.createdAt)
            stmt.executeUpdate() > 0
        }
    }

    override fun selectFirstAvailableWithLock(
        conn: SafeConnection,
        kind: String,
        now: Instant,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT
                "id",
                "pKey",
                "pKind",
                "payload",
                "scheduledAt",
                "scheduledAtInitially",
                "lockUuid",
                "createdAt"
            FROM "$tableName"
            WHERE "pKind" = ? AND "scheduledAt" <= ?
            ORDER BY "scheduledAt"
            LIMIT 1
            FOR UPDATE
            """

        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, kind)
            stmt.setEpochMillis(2, now)
            stmt.executeQuery().use { rs ->
                if (rs.next()) {
                    rs.toDBTableRowWithId()
                } else {
                    null
                }
            }
        }
    }

    override fun acquireManyOptimistically(
        conn: SafeConnection,
        kind: String,
        limit: Int,
        lockUuid: String,
        timeout: Duration,
        now: Instant,
    ): Int {
        require(limit > 0) { "Limit must be > 0" }
        val expireAt = now.plus(timeout)

        val sql =
            """
            UPDATE "$tableName"
            SET "lockUuid" = ?,
                "scheduledAt" = ?
            WHERE "id" IN (
                SELECT "id"
                FROM "$tableName"
                WHERE "pKind" = ? AND "scheduledAt" <= ?
                ORDER BY "scheduledAt", "id"
                LIMIT $limit
            )
            """

        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, lockUuid)
            stmt.setEpochMillis(2, expireAt)
            stmt.setString(3, kind)
            stmt.setEpochMillis(4, now)
            stmt.executeUpdate()
        }
    }
}
