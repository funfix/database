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

package org.funfix.delayedqueue.jvm.internals.jdbc.oracle

import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.SafeConnection
import org.funfix.delayedqueue.jvm.internals.jdbc.prepareStatement
import org.funfix.delayedqueue.jvm.internals.jdbc.toDBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.utils.Raise

/** Oracle-specific adapter. */
internal class OracleAdapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    override fun insertOneRow(conn: SafeConnection, row: DBTableRow): Boolean {
        // NOTE: this query can still throw an SQLException under concurrency,
        // because the NOT EXISTS check is not atomic. But this is still fine,
        // as we reduce the error rate, and the call-site does catch the SQLException.
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
            SELECT ?, ?, ?, ?, ?, ?
            FROM dual
            WHERE NOT EXISTS (
                SELECT 1
                FROM "$tableName"
                WHERE "pKey" = ? AND "pKind" = ?
            )
            """

        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, row.pKey)
            stmt.setString(2, row.pKind)
            stmt.setBytes(3, row.payload)
            stmt.setEpochMillis(4, row.scheduledAt)
            stmt.setEpochMillis(5, row.scheduledAtInitially)
            stmt.setEpochMillis(6, row.createdAt)
            stmt.setString(7, row.pKey)
            stmt.setString(8, row.pKind)
            stmt.executeUpdate() > 0
        }
    }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
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
            WHERE "pKey" = ? AND "pKind" = ? AND ROWNUM <= 1
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

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    override fun selectByKey(conn: SafeConnection, kind: String, key: String): DBTableRowWithId? {
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
            WHERE "pKey" = ? AND "pKind" = ? AND ROWNUM <= 1
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

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
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
            WHERE ROWID IN (
                SELECT ROWID
                FROM "$tableName"
                WHERE "pKind" = ? AND "scheduledAt" <= ?
                ORDER BY "scheduledAt"
                FETCH FIRST 1 ROWS ONLY
            )
            FOR UPDATE SKIP LOCKED
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

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
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

        val selectSql =
            """
            SELECT "id"
            FROM "$tableName"
            WHERE ROWID IN (
                SELECT ROWID
                FROM "$tableName"
                WHERE "pKind" = ? AND "scheduledAt" <= ?
                ORDER BY "scheduledAt"
                FETCH FIRST $limit ROWS ONLY
            )
            FOR UPDATE SKIP LOCKED
            """

        val ids =
            conn.prepareStatement(selectSql) { stmt ->
                stmt.setString(1, kind)
                stmt.setEpochMillis(2, now)
                stmt.executeQuery().use { rs ->
                    val results = mutableListOf<Long>()
                    while (rs.next()) {
                        results.add(rs.getLong("id"))
                    }
                    results
                }
            }

        if (ids.isEmpty()) return 0

        val placeholders = ids.joinToString(",") { "?" }
        val updateSql =
            """
            UPDATE "$tableName"
            SET
                "lockUuid" = ?,
                "scheduledAt" = ?
            WHERE "id" IN ($placeholders)
            """

        return conn.prepareStatement(updateSql) { stmt ->
            stmt.setString(1, lockUuid)
            stmt.setEpochMillis(2, expireAt)
            ids.forEachIndexed { index, id -> stmt.setLong(index + 3, id) }
            stmt.executeUpdate()
        }
    }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    override fun selectAllAvailableWithLock(
        conn: SafeConnection,
        lockUuid: String,
        count: Int,
        offsetId: Long?,
    ): List<DBTableRowWithId> {
        val offsetClause = offsetId?.let { "AND \"id\" > ?" } ?: ""
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
            FROM (
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
                WHERE "lockUuid" = ? $offsetClause
                ORDER BY "id"
            )
            WHERE ROWNUM <= $count
            """

        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, lockUuid)
            offsetId?.let { stmt.setLong(2, it) }
            stmt.executeQuery().use { rs ->
                val results = mutableListOf<DBTableRowWithId>()
                while (rs.next()) {
                    results.add(rs.toDBTableRowWithId())
                }
                results
            }
        }
    }
}
