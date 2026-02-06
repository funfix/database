package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver

/**
 * Describes actual SQL queries executed — can be overridden to provide driver-specific queries.
 *
 * This allows for database-specific optimizations like MS-SQL's `WITH (UPDLOCK, READPAST)` or
 * different `LIMIT` syntax across databases.
 *
 * @property driver the JDBC driver this adapter is for
 * @property tableName the name of the delayed queue table
 */
internal sealed class SQLVendorAdapter(val driver: JdbcDriver, protected val tableName: String) {
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
                stmt.setBytes(3, row.payload)
                stmt.setEpochMillis(4, row.scheduledAt)
                stmt.setEpochMillis(5, row.scheduledAtInitially)
                row.lockUuid?.let { stmt.setString(6, it) }
                    ?: stmt.setNull(6, java.sql.Types.VARCHAR)
                stmt.setEpochMillis(7, row.createdAt)
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
                lockUuid = ?,
                createdAt = ?
            WHERE pKey = ?
              AND pKind = ?
              AND scheduledAtInitially = ?
              AND createdAt = ?
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setBytes(1, updatedRow.payload)
            stmt.setEpochMillis(2, updatedRow.scheduledAt)
            stmt.setEpochMillis(3, updatedRow.scheduledAtInitially)
            stmt.setString(4, updatedRow.lockUuid)
            stmt.setEpochMillis(5, updatedRow.createdAt)
            stmt.setString(6, currentRow.pKey)
            stmt.setString(7, currentRow.pKind)
            stmt.setEpochMillis(8, currentRow.scheduledAtInitially)
            stmt.setEpochMillis(9, currentRow.createdAt)
            stmt.executeUpdate() > 0
        }
    }

    /** Selects one row by its key. */
    open fun selectByKey(connection: Connection, kind: String, key: String): DBTableRowWithId? {
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

    /**
     * Selects one row by its key with a lock (FOR UPDATE).
     *
     * This method is used during offer updates to prevent concurrent modifications.
     * Database-specific implementations may use different locking mechanisms:
     * - MS-SQL: WITH (UPDLOCK)
     * - HSQLDB: Falls back to plain SELECT (limited row-level locking support)
     */
    abstract fun selectForUpdateOneRow(
        connection: Connection,
        kind: String,
        key: String,
    ): DBTableRowWithId?

    /**
     * Searches for existing keys from a provided list.
     *
     * Returns the subset of keys that already exist in the database. This is used by batch
     * operations to avoid N+1 queries.
     */
    fun searchAvailableKeys(connection: Connection, kind: String, keys: List<String>): Set<String> {
        if (keys.isEmpty()) return emptySet()

        // Build IN clause with placeholders
        val placeholders = keys.joinToString(",") { "?" }
        val sql = "SELECT pKey FROM $tableName WHERE pKind = ? AND pKey IN ($placeholders)"

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, kind)
            keys.forEachIndexed { index, key -> stmt.setString(index + 2, key) }
            stmt.executeQuery().use { rs ->
                val existingKeys = mutableSetOf<String>()
                while (rs.next()) {
                    existingKeys.add(rs.getString("pKey"))
                }
                existingKeys
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

    /** Deletes a row by its fingerprint (id and createdAt). */
    fun deleteRowByFingerprint(connection: Connection, row: DBTableRowWithId): Boolean {
        val sql =
            """
            DELETE FROM $tableName
            WHERE id = ? AND createdAt = ?
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setLong(1, row.id)
            stmt.setEpochMillis(2, row.data.createdAt)
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
    open fun selectAllAvailableWithLock(
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
    /** Acquires a row by updating its scheduledAt and lockUuid. */
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
              AND scheduledAt = ?
            """

        return connection.prepareStatement(sql).use { stmt ->
            stmt.setEpochMillis(1, expireAt)
            stmt.setString(2, lockUuid)
            stmt.setString(3, row.pKey)
            stmt.setString(4, row.pKind)
            stmt.setEpochMillis(5, row.scheduledAt)
            stmt.executeUpdate() > 0
        }
    }

    protected fun PreparedStatement.setEpochMillis(index: Int, instant: Instant) {
        setLong(index, instant.toEpochMilli())
    }

    companion object {
        /** Creates the appropriate vendor adapter for the given JDBC driver. */
        fun create(driver: JdbcDriver, tableName: String): SQLVendorAdapter =
            when (driver) {
                JdbcDriver.HSQLDB -> HSQLDBAdapter(driver, tableName)
                JdbcDriver.Sqlite -> SqliteAdapter(driver, tableName)
                JdbcDriver.MsSqlServer -> MsSqlServerAdapter(driver, tableName)
            }
    }
}

/** HSQLDB-specific adapter. */
private class HSQLDBAdapter(driver: JdbcDriver, tableName: String) :
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

/** MS-SQL-specific adapter. */
private class MsSqlServerAdapter(driver: JdbcDriver, tableName: String) :
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

/**
 * SQLite-specific adapter.
 *
 * SQLite has limited concurrency support — it uses database-level locking (WAL mode helps for
 * concurrent reads). There is no SELECT FOR UPDATE or row-level locking. Instead, we rely on:
 * - `INSERT OR IGNORE` for conditional inserts (best performance idiom for SQLite)
 * - `LIMIT` syntax (same as HSQLDB)
 * - Optimistic locking via compare-and-swap UPDATE patterns
 */
private class SqliteAdapter(driver: JdbcDriver, tableName: String) :
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

/** MS-SQL-specific adapter. */
private class MsSqlServerAdapter(driver: JdbcDriver, tableName: String) :
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

/** Extension function to convert ResultSet to DBTableRowWithId. */
private fun ResultSet.toDBTableRowWithId(): DBTableRowWithId =
    DBTableRowWithId(
        id = getLong("id"),
        data =
            DBTableRow(
                pKey = getString("pKey"),
                pKind = getString("pKind"),
                payload = getBytes("payload"),
                scheduledAt = getInstant("scheduledAt"),
                scheduledAtInitially = getInstant("scheduledAtInitially"),
                lockUuid = getString("lockUuid"),
                createdAt = getInstant("createdAt"),
            ),
    )

private fun ResultSet.getInstant(columnLabel: String): Instant {
    return Instant.ofEpochMilli(getLong(columnLabel))
}
