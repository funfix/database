package org.funfix.delayedqueue.jvm.internals.jdbc.hsqldb

import java.sql.Connection
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.toDBTableRowWithId

/** HSQLDB-specific adapter. */
internal class HSQLDBAdapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

    override fun selectForUpdateOneRow(
        connection: Connection,
        kind: String,
        key: String,
    ): DBTableRowWithId? {
        // HSQLDB has limited row-level locking support, so we fall back to plain SELECT.
        // This matches the original Scala implementation's behavior for HSQLDB.
        return selectByKey(connection, kind, key)
    }

    override fun insertOneRow(connection: Connection, row: DBTableRow): Boolean {
        val sql =
            """
            INSERT INTO $tableName
            (pKey, pKind, payload, scheduledAt, scheduledAtInitially, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """

        return try {
            connection.prepareStatement(sql).use { stmt ->
                stmt.setString(1, row.pKey)
                stmt.setString(2, row.pKind)
                stmt.setBytes(3, row.payload)
                stmt.setEpochMillis(4, row.scheduledAt)
                stmt.setEpochMillis(5, row.scheduledAtInitially)
                stmt.setEpochMillis(6, row.createdAt)
                stmt.executeUpdate() > 0
            }
        } catch (e: Exception) {
            // If it's a duplicate key violation, return false (key already exists)
            // This matches the original Scala implementation's behavior:
            // insertIntoTable(...).recover { case SQLExceptionExtractors.DuplicateKey(_) => false }
            if (HSQLDBFilters.duplicateKey.matches(e)) {
                false
            } else {
                throw e
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
