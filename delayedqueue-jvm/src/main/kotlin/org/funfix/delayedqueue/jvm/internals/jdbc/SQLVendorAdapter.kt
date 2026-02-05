package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.Connection
import java.sql.ResultSet
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.funfix.delayedqueue.jvm.JdbcDriver

/**
 * Truncates an Instant to seconds precision.
 *
 * This matches the old Scala implementation's DBColumnOffsetDateTime(ChronoUnit.SECONDS) behavior
 * and is critical for database compatibility.
 */
private fun truncateToSeconds(instant: Instant): Instant = instant.truncatedTo(ChronoUnit.SECONDS)

/**
 * Describes actual SQL queries executed — can be overridden to provide driver-specific queries.
 *
 * This allows for database-specific optimizations like MS-SQL's `WITH (UPDLOCK, READPAST)` or
 * different `LIMIT` syntax across databases.
 */
internal sealed class SQLVendorAdapter(protected val tableName: String) {
    /** Checks if a key exists in the database. */
    fun checkIfKeyExists(connection: Connection, key: String, kind: String): Boolean {
        val sql = "SELECT 1 FROM $tableName WHERE pKey = ? AND pKind = ?"
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, key)
            stmt.setString(2, kind)
            stmt.executeQuery().use { rs -> rs.next() }
        }
    }

    /**
     * Inserts a single row into the database. Returns true if inserted, false if key already
     * exists.
     */
    abstract fun insertOneRow(connection: Connection, row: DBTableRow): Boolean

    /**
     * Inserts multiple rows in a batch. Returns the list of keys that were successfully inserted.
     */
    fun insertBatch(connection: Connection, rows: List<DBTableRow>): List<String> {
        if (rows.isEmpty()) return emptyList()

        val sql =
            """
            INSERT INTO $tableName
            (pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """

        val inserted = mutableListOf<String>()
        connection.prepareStatement(sql).use { stmt ->
            for (row in rows) {
                stmt.setString(1, row.pKey)
                stmt.setString(2, row.pKind)
                stmt.setString(3, row.payload)
                stmt.setTimestamp(4, java.sql.Timestamp.from(row.scheduledAt))
                stmt.setTimestamp(5, java.sql.Timestamp.from(row.scheduledAtInitially))
                row.lockUuid?.let { stmt.setString(6, it) }
                    ?: stmt.setNull(6, java.sql.Types.VARCHAR)
                stmt.setTimestamp(7, java.sql.Timestamp.from(row.createdAt))
                stmt.addBatch()
            }
            val results = stmt.executeBatch()
            results.forEachIndexed { index, result ->
                if (result > 0) {
                    inserted.add(rows[index].pKey)
                }
            }
        }
        return inserted
    }

    /**
     * Updates an existing row with optimistic locking (compare-and-swap). Only updates if the
     * current row matches what's in the database.
     *
     * Uses timestamp truncation to handle precision differences between SELECT and UPDATE.
     */
    fun guardedUpdate(
        connection: Connection,
        currentRow: DBTableRow,
        updatedRow: DBTableRow,
    ): Boolean {
        val sql =
            """
            UPDATE $tableName
            SET payload = ?,
                scheduledAt = ?,
                scheduledAtInitially = ?,
                createdAt = ?
            WHERE pKey = ?
              AND pKind = ?
              AND scheduledAtInitially IN (?, ?)
              AND createdAt IN (?, ?)
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, updatedRow.payload)
            stmt.setTimestamp(2, java.sql.Timestamp.from(updatedRow.scheduledAt))
            stmt.setTimestamp(3, java.sql.Timestamp.from(updatedRow.scheduledAtInitially))
            stmt.setTimestamp(4, java.sql.Timestamp.from(updatedRow.createdAt))
            stmt.setString(5, currentRow.pKey)
            stmt.setString(6, currentRow.pKind)
            // scheduledAtInitially IN (truncated, full)
            stmt.setTimestamp(
                7,
                java.sql.Timestamp.from(truncateToSeconds(currentRow.scheduledAtInitially)),
            )
            stmt.setTimestamp(8, java.sql.Timestamp.from(currentRow.scheduledAtInitially))
            // createdAt IN (truncated, full)
            stmt.setTimestamp(9, java.sql.Timestamp.from(truncateToSeconds(currentRow.createdAt)))
            stmt.setTimestamp(10, java.sql.Timestamp.from(currentRow.createdAt))
            stmt.executeUpdate() > 0
        }
    }

    /** Selects one row by its key. */
    fun selectByKey(connection: Connection, kind: String, key: String): DBTableRowWithId? {
        val sql =
            """
            SELECT id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WHERE pKey = ? AND pKind = ?
            LIMIT 1
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

    /** Deletes one row by key and kind. */
    fun deleteOneRow(connection: Connection, key: String, kind: String): Boolean {
        val sql = "DELETE FROM $tableName WHERE pKey = ? AND pKind = ?"
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, key)
            stmt.setString(2, kind)
            stmt.executeUpdate() > 0
        }
    }

    /** Deletes rows with a specific lock UUID. */
    fun deleteRowsWithLock(connection: Connection, lockUuid: String): Int {
        val sql = "DELETE FROM $tableName WHERE lockUuid = ?"
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, lockUuid)
            stmt.executeUpdate()
        }
    }

    /**
     * Deletes a row by its fingerprint (id and createdAt). Uses timestamp truncation to handle
     * precision differences.
     */
    fun deleteRowByFingerprint(connection: Connection, row: DBTableRowWithId): Boolean {
        val sql =
            """
            DELETE FROM $tableName
            WHERE id = ? AND createdAt IN (?, ?)
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setLong(1, row.id)
            // createdAt IN (truncated, full)
            stmt.setTimestamp(2, java.sql.Timestamp.from(truncateToSeconds(row.data.createdAt)))
            stmt.setTimestamp(3, java.sql.Timestamp.from(row.data.createdAt))
            stmt.executeUpdate() > 0
        }
    }

    /** Deletes all rows with a specific kind (used for cleanup in tests). */
    fun dropAllMessages(connection: Connection, kind: String): Int {
        val sql = "DELETE FROM $tableName WHERE pKind = ?"
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, kind)
            stmt.executeUpdate()
        }
    }

    /**
     * Deletes cron messages matching a specific config hash and key prefix. Used by uninstallTick
     * to remove the current cron configuration.
     */
    fun deleteCurrentCron(
        connection: Connection,
        kind: String,
        keyPrefix: String,
        configHash: String,
    ): Int {
        val sql = "DELETE FROM $tableName WHERE pKind = ? AND pKey LIKE ?"
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, kind)
            stmt.setString(2, "$keyPrefix/$configHash%")
            stmt.executeUpdate()
        }
    }

    /**
     * Deletes ALL cron messages with a given prefix (ignoring config hash). This is used as a
     * fallback or for complete cleanup of a prefix.
     */
    fun deleteAllForPrefix(connection: Connection, kind: String, keyPrefix: String): Int {
        val sql = "DELETE FROM $tableName WHERE pKind = ? AND pKey LIKE ?"
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, kind)
            stmt.setString(2, "$keyPrefix/%")
            stmt.executeUpdate()
        }
    }

    /**
     * Deletes OLD cron messages (those with a DIFFERENT config hash than the current one). Used by
     * installTick to remove outdated configurations while preserving the current one. This avoids
     * wasteful deletions when the configuration hasn't changed.
     */
    fun deleteOldCron(
        connection: Connection,
        kind: String,
        keyPrefix: String,
        configHash: String,
    ): Int {
        val sql =
            """
            DELETE FROM $tableName
            WHERE pKind = ?
              AND pKey LIKE ?
              AND pKey NOT LIKE ?
            """
        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, kind)
            stmt.setString(2, "$keyPrefix/%")
            stmt.setString(3, "$keyPrefix/$configHash%")
            stmt.executeUpdate()
        }
    }

    /**
     * Acquires many messages optimistically by updating them with a lock. Returns the number of
     * messages acquired.
     */
    abstract fun acquireManyOptimistically(
        connection: Connection,
        kind: String,
        limit: Int,
        lockUuid: String,
        timeout: Duration,
        now: Instant,
    ): Int

    /** Selects the first available message for processing (with locking if supported). */
    abstract fun selectFirstAvailableWithLock(
        connection: Connection,
        kind: String,
        now: Instant,
    ): DBTableRowWithId?

    /** Selects all messages with a specific lock UUID. */
    fun selectAllAvailableWithLock(
        connection: Connection,
        lockUuid: String,
        count: Int,
        offsetId: Long?,
    ): List<DBTableRowWithId> {
        val offsetClause = offsetId?.let { "AND id > ?" } ?: ""
        val sql =
            """
            SELECT id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WHERE lockUuid = ? $offsetClause
            ORDER BY id
            LIMIT $count
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

    /**
     * Acquires a specific row by updating its scheduledAt and lockUuid. Returns true if the row was
     * successfully acquired.
     */
    /**
     * Acquires a row by updating its scheduledAt and lockUuid. Uses timestamp truncation to handle
     * precision differences.
     */
    fun acquireRowByUpdate(
        connection: Connection,
        row: DBTableRow,
        lockUuid: String,
        timeout: Duration,
        now: Instant,
    ): Boolean {
        val expireAt = now.plus(timeout)
        val sql =
            """
            UPDATE $tableName
            SET scheduledAt = ?,
                lockUuid = ?
            WHERE pKey = ?
              AND pKind = ?
              AND scheduledAt IN (?, ?)
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setTimestamp(1, java.sql.Timestamp.from(expireAt))
            stmt.setString(2, lockUuid)
            stmt.setString(3, row.pKey)
            stmt.setString(4, row.pKind)
            // scheduledAt IN (exact, truncated)
            stmt.setTimestamp(5, java.sql.Timestamp.from(row.scheduledAt))
            stmt.setTimestamp(6, java.sql.Timestamp.from(truncateToSeconds(row.scheduledAt)))
            stmt.executeUpdate() > 0
        }
    }

    companion object {
        /** Creates the appropriate vendor adapter for the given JDBC driver. */
        fun create(driver: JdbcDriver, tableName: String): SQLVendorAdapter =
            when (driver) {
                JdbcDriver.HSQLDB -> HSQLDBAdapter(tableName)
                JdbcDriver.MsSqlServer,
                JdbcDriver.Sqlite -> TODO("MS-SQL and SQLite support not yet implemented")
            }
    }
}

/** HSQLDB-specific adapter. */
private class HSQLDBAdapter(tableName: String) : SQLVendorAdapter(tableName) {
    override fun insertOneRow(connection: Connection, row: DBTableRow): Boolean {
        // HSQLDB doesn't have INSERT IGNORE, so we check first
        if (checkIfKeyExists(connection, row.pKey, row.pKind)) {
            return false
        }

        val sql =
            """
            INSERT INTO $tableName
            (pKey, pKind, payload, scheduledAt, scheduledAtInitially, createdAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, row.pKey)
            stmt.setString(2, row.pKind)
            stmt.setString(3, row.payload)
            stmt.setTimestamp(4, java.sql.Timestamp.from(row.scheduledAt))
            stmt.setTimestamp(5, java.sql.Timestamp.from(row.scheduledAtInitially))
            stmt.setTimestamp(6, java.sql.Timestamp.from(row.createdAt))
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
            SELECT TOP 1
                id, pKey, pKind, payload, scheduledAt, scheduledAtInitially, lockUuid, createdAt
            FROM $tableName
            WHERE pKind = ? AND scheduledAt <= ?
            ORDER BY scheduledAt
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, kind)
            stmt.setTimestamp(2, java.sql.Timestamp.from(now))
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
            stmt.setTimestamp(2, java.sql.Timestamp.from(expireAt))
            stmt.setString(3, kind)
            stmt.setTimestamp(4, java.sql.Timestamp.from(now))
            stmt.executeUpdate()
        }
    }
}

/** Extension function to convert ResultSet to DBTableRowWithId. */
private fun ResultSet.toDBTableRowWithId(): DBTableRowWithId =
    DBTableRowWithId(
        id = getLong("id"),
        data =
            DBTableRow(
                pKey = getString("pKey"),
                pKind = getString("pKind"),
                payload = getString("payload"),
                scheduledAt = getTimestamp("scheduledAt").toInstant(),
                scheduledAtInitially = getTimestamp("scheduledAtInitially").toInstant(),
                lockUuid = getString("lockUuid"),
                createdAt = getTimestamp("createdAt").toInstant(),
            ),
    )
