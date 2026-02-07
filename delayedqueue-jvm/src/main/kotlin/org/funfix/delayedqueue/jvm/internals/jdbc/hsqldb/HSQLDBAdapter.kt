package org.funfix.delayedqueue.jvm.internals.jdbc.hsqldb

import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.SafeConnection
import org.funfix.delayedqueue.jvm.internals.jdbc.prepareStatement
import org.funfix.delayedqueue.jvm.internals.jdbc.quote
import org.funfix.delayedqueue.jvm.internals.jdbc.toDBTableRowWithId
import org.funfix.delayedqueue.jvm.internals.utils.Raise

/** HSQLDB-specific adapter. */
internal class HSQLDBAdapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    override fun selectForUpdateOneRow(
        conn: SafeConnection,
        kind: String,
        key: String,
    ): DBTableRowWithId? {
        // HSQLDB has limited row-level locking support, so we fall back to plain SELECT.
        // This matches the original Scala implementation's behavior for HSQLDB.
        return selectByKey(conn, kind, key)
    }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    override fun insertOneRow(conn: SafeConnection, row: DBTableRow): Boolean {
        val sql =
            """
            INSERT INTO ${conn.quote(tableName)}
            (
                ${conn.quote("pKey")}, 
                ${conn.quote("pKind")}, 
                ${conn.quote("payload")}, 
                ${conn.quote("scheduledAt")}, 
                ${conn.quote("scheduledAtInitially")}, 
                ${conn.quote("createdAt")}
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """

        return try {
            conn.prepareStatement(sql) { stmt ->
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

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    override fun selectFirstAvailableWithLock(
        conn: SafeConnection,
        kind: String,
        now: Instant,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT 
                ${conn.quote("id")}, 
                ${conn.quote("pKey")}, 
                ${conn.quote("pKind")}, 
                ${conn.quote("payload")}, 
                ${conn.quote("scheduledAt")}, 
                ${conn.quote("scheduledAtInitially")}, 
                ${conn.quote("lockUuid")},
                ${conn.quote("createdAt")}
            FROM ${conn.quote(tableName)}
            WHERE ${conn.quote("pKind")} = ? AND ${conn.quote("scheduledAt")} <= ?
            ORDER BY ${conn.quote("scheduledAt")}
            FETCH FIRST 1 ROWS ONLY
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

        val sql =
            """
            UPDATE ${conn.quote(tableName)}
            SET ${conn.quote("lockUuid")} = ?,
                ${conn.quote("scheduledAt")} = ?
            WHERE ${conn.quote("id")} IN (
                SELECT ${conn.quote("id")}
                FROM ${conn.quote(tableName)}
                WHERE ${conn.quote("pKind")} = ? AND ${conn.quote("scheduledAt")} <= ?
                ORDER BY ${conn.quote("scheduledAt")}
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
