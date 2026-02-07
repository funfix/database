package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.h2.H2Adapter
import org.funfix.delayedqueue.jvm.internals.jdbc.hsqldb.HSQLDBAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.mariadb.MariaDBAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.mssql.MsSqlServerAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.mysql.MySQLAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.oracle.OracleAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.postgres.PostgreSQLAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.sqlite.SqliteAdapter
import org.funfix.delayedqueue.jvm.internals.utils.Raise

/**
 * Describes actual SQL queries executed — can be overridden to provide driver-specific queries.
 *
 * This allows for database-specific optimizations like MS-SQL's `WITH (UPDLOCK, READPAST)` or
 * different `LIMIT` syntax across databases.
 *
 * @property driver the JDBC driver this adapter is for
 * @property tableName the name of the delayed queue table
 */
internal abstract class SQLVendorAdapter(val driver: JdbcDriver, protected val tableName: String) {
    /** Checks if a key exists in the database. */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun checkIfKeyExists(conn: SafeConnection, key: String, kind: String): Boolean {
        val sql =
            """
            SELECT 1 FROM ${conn.quote(tableName)} 
            WHERE ${conn.quote("pKey")} = ? AND ${conn.quote("pKind")} = ?
            """
        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, key)
            stmt.setString(2, kind)
            stmt.executeQuery().use { rs -> rs.next() }
        }
    }

    /**
     * Inserts a single row into the database. Returns true if inserted, false if key already
     * exists.
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    abstract fun insertOneRow(conn: SafeConnection, row: DBTableRow): Boolean

    /**
     * Inserts multiple rows in a batch. Returns the list of keys that were successfully inserted.
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun insertBatch(conn: SafeConnection, rows: List<DBTableRow>): List<String> {
        if (rows.isEmpty()) return emptyList()

        val sqlPrefix =
            """
            INSERT INTO ${conn.quote(tableName)}
            (
                ${conn.quote("pKey")}, 
                ${conn.quote("pKind")}, 
                ${conn.quote("payload")}, 
                ${conn.quote("scheduledAt")}, 
                ${conn.quote("scheduledAtInitially")}, 
                ${conn.quote("lockUuid")}, 
                ${conn.quote("createdAt")}
            )
            VALUES
            """

        val inserted = mutableListOf<String>()
        for (chunk in rows.chunked(200)) {
            if (chunk.isEmpty()) continue
            val placeholders = chunk.joinToString(",\n") { "    (?, ?, ?, ?, ?, ?, ?)" }
            val sql = sqlPrefix + "\n" + placeholders

            conn.prepareStatement(sql) { stmt ->
                var paramIndex = 1
                for (row in chunk) {
                    stmt.setString(paramIndex++, row.pKey)
                    stmt.setString(paramIndex++, row.pKind)
                    stmt.setBytes(paramIndex++, row.payload)
                    stmt.setEpochMillis(paramIndex++, row.scheduledAt)
                    stmt.setEpochMillis(paramIndex++, row.scheduledAtInitially)
                    if (row.lockUuid != null) {
                        stmt.setString(paramIndex++, row.lockUuid)
                    } else {
                        stmt.setNull(paramIndex++, Types.VARCHAR)
                    }
                    stmt.setEpochMillis(paramIndex++, row.createdAt)
                }
                val wereInserted =
                    try {
                        stmt.execute()
                        true
                    } catch (e: Exception) {
                        if (!filtersForDriver(driver).duplicateKey.matches(e)) throw e
                        false
                    }
                if (wereInserted) {
                    for (row in chunk) {
                        inserted.add(row.pKey)
                    }
                }
            }
        }
        return inserted
    }

    /**
     * Updates an existing row with optimistic locking (compare-and-swap). Only updates if the
     * current row matches what's in the database.
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun guardedUpdate(
        conn: SafeConnection,
        currentRow: DBTableRow,
        updatedRow: DBTableRow,
    ): Boolean {
        val sql =
            """
            UPDATE ${conn.quote(tableName)}
            SET 
                ${conn.quote("payload")} = ?,
                ${conn.quote("scheduledAt")} = ?,
                ${conn.quote("scheduledAtInitially")} = ?,
                ${conn.quote("lockUuid")} = ?,
                ${conn.quote("createdAt")} = ?
            WHERE 
                ${conn.quote("pKey")} = ?
                AND ${conn.quote("pKind")} = ?
                AND ${conn.quote("scheduledAtInitially")} = ?
                AND ${conn.quote("createdAt")} = ?
            """

        return conn.prepareStatement(sql) { stmt ->
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
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    open fun selectByKey(conn: SafeConnection, kind: String, key: String): DBTableRowWithId? {
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

    /**
     * Selects one row by its key with a lock (FOR UPDATE).
     *
     * This method is used during offer updates to prevent concurrent modifications.
     * Database-specific implementations may use different locking mechanisms:
     * - MS-SQL: WITH (UPDLOCK)
     * - HSQLDB: Falls back to plain SELECT (limited row-level locking support)
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    abstract fun selectForUpdateOneRow(
        conn: SafeConnection,
        kind: String,
        key: String,
    ): DBTableRowWithId?

    /**
     * Searches for existing keys from a provided list.
     *
     * Returns the subset of keys that already exist in the database. This is used by batch
     * operations to avoid N+1 queries.
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun searchAvailableKeys(conn: SafeConnection, kind: String, keys: List<String>): Set<String> {
        if (keys.isEmpty()) return emptySet()

        // Build IN clause with placeholders
        val placeholders = keys.joinToString(",") { "?" }
        val sql =
            """
            SELECT ${conn.quote("pKey")} 
            FROM ${conn.quote(tableName)} 
            WHERE 
                ${conn.quote("pKind")} = ? 
                AND ${conn.quote("pKey")} IN ($placeholders)
            """

        return conn.prepareStatement(sql) { stmt ->
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
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun deleteOneRow(conn: SafeConnection, key: String, kind: String): Boolean {
        val sql =
            """
            DELETE FROM ${conn.quote(tableName)} 
            WHERE 
                ${conn.quote("pKey")} = ? 
                AND ${conn.quote("pKind")} = ?
            """
        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, key)
            stmt.setString(2, kind)
            stmt.executeUpdate() > 0
        }
    }

    /** Deletes rows with a specific lock UUID. */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun deleteRowsWithLock(conn: SafeConnection, lockUuid: String): Int {
        val sql =
            """
            DELETE FROM ${conn.quote(tableName)} 
            WHERE ${conn.quote("lockUuid")} = ?
            """
        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, lockUuid)
            stmt.executeUpdate()
        }
    }

    /** Deletes a row by its fingerprint (id and createdAt). */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun deleteRowByFingerprint(conn: SafeConnection, row: DBTableRowWithId): Boolean {
        val sql =
            """
            DELETE FROM ${conn.quote(tableName)}
            WHERE 
                ${conn.quote("id")} = ? 
                AND ${conn.quote("scheduledAtInitially")} = ? 
                AND ${conn.quote("createdAt")} = ?
            """
        return conn.prepareStatement(sql) { stmt ->
            stmt.setLong(1, row.id)
            stmt.setEpochMillis(2, row.data.scheduledAtInitially)
            stmt.setEpochMillis(3, row.data.createdAt)
            stmt.executeUpdate() > 0
        }
    }

    /** Deletes all rows with a specific kind (used for cleanup in tests). */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun dropAllMessages(onnn: SafeConnection, kind: String): Int {
        val sql =
            """
            DELETE FROM ${onnn.quote(tableName)} 
            WHERE ${onnn.quote("pKind")} = ?
            """
        return onnn.prepareStatement(sql) { stmt ->
            stmt.setString(1, kind)
            stmt.executeUpdate()
        }
    }

    /**
     * Deletes cron messages matching a specific config hash and key prefix. Used by uninstallTick
     * to remove the current cron configuration.
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun deleteCurrentCron(
        conn: SafeConnection,
        kind: String,
        keyPrefix: String,
        configHash: String,
    ): Int {
        val sql =
            """
            DELETE FROM ${conn.quote(tableName)} 
            WHERE 
                ${conn.quote("pKind")} = ? 
                AND ${conn.quote("pKey")} LIKE ?
            """
        return conn.prepareStatement(sql) { stmt ->
            stmt.setString(1, kind)
            stmt.setString(2, "$keyPrefix/$configHash%")
            stmt.executeUpdate()
        }
    }

    /**
     * Deletes ALL cron messages with a given prefix (ignoring config hash). This is used as a
     * fallback or for complete cleanup of a prefix.
     */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun deleteAllForPrefix(conn: SafeConnection, kind: String, keyPrefix: String): Int {
        val sql =
            """
            DELETE FROM ${conn.quote(tableName)} 
            WHERE ${conn.quote("pKind")} = ? 
                AND ${conn.quote("pKey")} LIKE ?
            """
        return conn.prepareStatement(sql) { stmt ->
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
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun deleteOldCron(
        conn: SafeConnection,
        kind: String,
        keyPrefix: String,
        configHash: String,
    ): Int {
        val sql =
            """
            DELETE FROM ${conn.quote(tableName)}
            WHERE ${conn.quote("pKind")} = ?
              AND ${conn.quote("pKey")} LIKE ?
              AND ${conn.quote("pKey")} NOT LIKE ?
            """
        return conn.prepareStatement(sql) { stmt ->
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
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    abstract fun acquireManyOptimistically(
        conn: SafeConnection,
        kind: String,
        limit: Int,
        lockUuid: String,
        timeout: Duration,
        now: Instant,
    ): Int

    /** Selects the first available message for processing (with locking if supported). */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    abstract fun selectFirstAvailableWithLock(
        conn: SafeConnection,
        kind: String,
        now: Instant,
    ): DBTableRowWithId?

    /** Selects all messages with a specific lock UUID. */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    open fun selectAllAvailableWithLock(
        conn: SafeConnection,
        lockUuid: String,
        count: Int,
        offsetId: Long?,
    ): List<DBTableRowWithId> {
        val offsetClause = offsetId?.let { "AND ${conn.quote("id")} > ?" } ?: ""
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
            WHERE ${conn.quote("lockUuid")} = ? $offsetClause
            ORDER BY ${conn.quote("id")}
            LIMIT $count
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

    /**
     * Acquires a specific row by updating its scheduledAt and lockUuid. Returns true if the row was
     * successfully acquired.
     */
    /** Acquires a row by updating its scheduledAt and lockUuid. */
    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    fun acquireRowByUpdate(
        conn: SafeConnection,
        row: DBTableRow,
        lockUuid: String,
        timeout: Duration,
        now: Instant,
    ): Boolean {
        val expireAt = now.plus(timeout)
        val sql =
            """
            UPDATE ${conn.quote(tableName)}
            SET ${conn.quote("scheduledAt")} = ?,
                ${conn.quote("lockUuid")} = ?
            WHERE ${conn.quote("pKey")} = ?
              AND ${conn.quote("pKind")} = ?
              AND ${conn.quote("scheduledAt")} = ?
            """

        return conn.prepareStatement(sql) { stmt ->
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
                JdbcDriver.H2 -> H2Adapter(driver, tableName)
                JdbcDriver.Sqlite -> SqliteAdapter(driver, tableName)
                JdbcDriver.MsSqlServer -> MsSqlServerAdapter(driver, tableName)
                JdbcDriver.PostgreSQL -> PostgreSQLAdapter(driver, tableName)
                JdbcDriver.MariaDB -> MariaDBAdapter(driver, tableName)
                JdbcDriver.MySQL -> MySQLAdapter(driver, tableName)
                JdbcDriver.Oracle -> OracleAdapter(driver, tableName)
                else -> throw IllegalArgumentException("Unsupported JDBC driver: $driver")
            }
    }
}

/** Extension function to convert ResultSet to DBTableRowWithId. */
internal fun ResultSet.toDBTableRowWithId(): DBTableRowWithId =
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
