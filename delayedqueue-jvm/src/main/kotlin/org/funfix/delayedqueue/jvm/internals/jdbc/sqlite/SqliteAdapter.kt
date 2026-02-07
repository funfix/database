package org.funfix.delayedqueue.jvm.internals.jdbc.sqlite

import java.sql.Connection
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.toDBTableRowWithId

/**
 * SQLite-specific adapter.
 *
 * SQLite has limited concurrency support — it uses database-level locking (WAL mode helps for
 * concurrent reads). There is no SELECT FOR UPDATE or row-level locking. Instead, we rely on:
 * - `INSERT OR IGNORE` for conditional inserts (best performance idiom for SQLite)
 * - `LIMIT` syntax (same as HSQLDB)
 * - Optimistic locking via compare-and-swap UPDATE patterns
 */
internal class SqliteAdapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

    override fun selectForUpdateOneRow(
        connection: Connection,
        kind: String,
        key: String,
    ): DBTableRowWithId? {
        // SQLite has no row-level locking; fall back to plain SELECT like HSQLDB.
        return selectByKey(connection, kind, key)
    }

    override fun insertOneRow(connection: Connection, row: DBTableRow): Boolean {
        // INSERT OR IGNORE is the idiomatic SQLite way to skip duplicate key inserts.
        val sql =
            """
            INSERT OR IGNORE INTO $tableName
            (pKey, pKind, payload, scheduledAt, scheduledAtInitially, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """

        return connection.prepareStatement(sql).use { stmt ->
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
        connection: Connection,
        kind: String,
        now: Instant,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WHERE pKind = ? AND scheduledAt <= ?
            ORDER BY scheduledAt
            LIMIT 1
            """

        return connection.prepareStatement(sql).use { stmt ->
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
        connection: Connection,
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
            UPDATE $tableName
            SET lockUuid = ?,
                scheduledAt = ?
            WHERE id IN (
                SELECT id
                FROM $tableName
                WHERE pKind = ? AND scheduledAt <= ?
                ORDER BY scheduledAt
                LIMIT $limit
            )
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, lockUuid)
            stmt.setEpochMillis(2, expireAt)
            stmt.setString(3, kind)
            stmt.setEpochMillis(4, now)
            stmt.executeUpdate()
        }
    }
}
