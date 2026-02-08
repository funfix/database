package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.*

/**
 * Adapter for MySQL-compatible databases (MySQL and MariaDB).
 *
 * Both MySQL and MariaDB share the same SQL syntax and error codes, so they can use the same
 * adapter implementation.
 */
internal open class MySQLCompatibleAdapter(driver: JdbcDriver, tableName: String) :
    SQLVendorAdapter(driver, tableName) {

    @Throws(InterruptedException::class, SQLException::class)
    override fun insertOneRow(conn: SafeConnection, row: DBTableRow): Boolean {
        val sql =
            """
            INSERT IGNORE INTO `$tableName`
            (
                `pKey`, 
                `pKind`, 
                `payload`, 
                `scheduledAt`, 
                `scheduledAtInitially`, 
                `createdAt`
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

    @Throws(InterruptedException::class, SQLException::class)
    override fun selectForUpdateOneRow(
        conn: SafeConnection,
        kind: String,
        key: String,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT 
                `id`, 
                `pKey`, 
                `pKind`, 
                `payload`, 
                `scheduledAt`, 
                `scheduledAtInitially`, 
                `lockUuid`, 
                `createdAt`
            FROM `$tableName`
            WHERE `pKey` = ? AND `pKind` = ?
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

    @Throws(InterruptedException::class, SQLException::class)
    override fun selectFirstAvailableWithLock(
        conn: SafeConnection,
        kind: String,
        now: Instant,
    ): DBTableRowWithId? {
        val sql =
            """
            SELECT 
                `id`, 
                `pKey`, 
                `pKind`, 
                `payload`, 
                `scheduledAt`, 
                `scheduledAtInitially`, 
                `lockUuid`, 
                `createdAt`
            FROM `$tableName`
            WHERE `pKind` = ? AND `scheduledAt` <= ?
            ORDER BY `scheduledAt`
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

    @Throws(InterruptedException::class, SQLException::class)
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
            UPDATE `$tableName`
            SET `lockUuid` = ?,
                `scheduledAt` = ?
            WHERE `id` IN (
                SELECT `id` FROM (
                    SELECT `id`
                    FROM `$tableName`
                    WHERE `pKind` = ? AND `scheduledAt` <= ?
                    ORDER BY `scheduledAt`
                    LIMIT $limit
                    FOR UPDATE SKIP LOCKED
                ) AS subq
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
