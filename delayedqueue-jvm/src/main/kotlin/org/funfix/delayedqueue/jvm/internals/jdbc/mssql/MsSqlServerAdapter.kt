package org.funfix.delayedqueue.jvm.internals.jdbc.mssql

import java.sql.Connection
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.toDBTableRowWithId

/** MS-SQL-specific adapter. */
internal class MsSqlServerAdapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

    override fun insertOneRow(connection: Connection, row: DBTableRow): Boolean {
        val sql =
            """
            IF NOT EXISTS (
                SELECT 1 FROM $tableName WHERE pKey = ? AND pKind = ?
            )
            BEGIN
                INSERT INTO $tableName
                (pKey, pKind, payload, scheduledAt, scheduledAtInitially, createdAt)
                VALUES (?, ?, ?, ?, ?, ?)
            END
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, row.pKey)
            stmt.setString(2, row.pKind)
            stmt.setString(3, row.pKey)
            stmt.setString(4, row.pKind)
            stmt.setBytes(5, row.payload)
            stmt.setEpochMillis(6, row.scheduledAt)
            stmt.setEpochMillis(7, row.scheduledAtInitially)
            stmt.setEpochMillis(8, row.createdAt)
            stmt.executeUpdate() > 0
        }
    }

    override fun selectForUpdateOneRow(
        connection: Connection,
        kind: String,
        key: String,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT TOP 1
                id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WITH (UPDLOCK)
            WHERE pKey = ? AND pKind = ?
            """

        return connection.prepareStatement(sql).use { stmt ->
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

    override fun selectFirstAvailableWithLock(
        connection: Connection,
        kind: String,
        now: Instant,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT TOP 1
                id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WITH (UPDLOCK, READPAST)
            WHERE pKind = ? AND scheduledAt <= ?
            ORDER BY scheduledAt
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
                SELECT TOP $limit id
                FROM $tableName
                WITH (UPDLOCK, READPAST)
                WHERE pKind = ? AND scheduledAt <= ?
                ORDER BY scheduledAt
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

    override fun selectByKey(connection: Connection, kind: String, key: String): DBTableRowWithId? {
        val sql =
            """
            SELECT TOP 1
                id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WHERE pKey = ? AND pKind = ?
            """

        return connection.prepareStatement(sql).use { stmt ->
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

    override fun selectAllAvailableWithLock(
        connection: Connection,
        lockUuid: String,
        count: Int,
        offsetId: Long?,
    ): List<DBTableRowWithId> {
        val offsetClause = offsetId?.let { "AND id > ?" } ?: ""
        val sql =
            """
            SELECT TOP $count
                id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WHERE lockUuid = ? $offsetClause
            ORDER BY id
            """

        return connection.prepareStatement(sql).use { stmt ->
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
