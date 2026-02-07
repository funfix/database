package org.funfix.delayedqueue.jvm.internals.jdbc.postgres

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

/** PostgreSQL-specific adapter. */
internal class PostgreSQLAdapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

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
            ON CONFLICT 
                (${conn.quote("pKey")}, ${conn.quote("pKind")}) 
                DO NOTHING
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

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    override fun selectForUpdateOneRow(
        conn: SafeConnection,
        kind: String,
        key: String,
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
            WHERE ${conn.quote("pKey")} = ? AND ${conn.quote("pKind")} = ?
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
            LIMIT 1
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

        val sql =
            """
            UPDATE ${conn.quote(tableName)}
            SET 
                ${conn.quote("lockUuid")} = ?,
                ${conn.quote("scheduledAt")} = ?
            WHERE ${conn.quote("id")} IN (
                SELECT ${conn.quote("id")}
                FROM ${conn.quote(tableName)}
                WHERE 
                    ${conn.quote("pKind")} = ? AND 
                    ${conn.quote("scheduledAt")} <= ?
                ORDER BY ${conn.quote("scheduledAt")}
                LIMIT $limit
                FOR UPDATE SKIP LOCKED
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
