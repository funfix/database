package org.funfix.delayedqueue.jvm.internals.jdbc

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MsSqlServerAdapterSqlTests {
    private val tableName = "dq_table"

    @Test
    fun `insert uses IF NOT EXISTS pattern`() {
        val capture = SqlCapture()
        val connection = capture.connection(executeUpdateResult = 1)
        val adapter = SQLVendorAdapter.create(JdbcDriver.MsSqlServer, tableName)
        val row =
            DBTableRow(
                pKey = "key-1",
                pKind = "kind-1",
                payload = "payload".toByteArray(),
                scheduledAt = Instant.parse("2024-01-01T00:00:00Z"),
                scheduledAtInitially = Instant.parse("2024-01-01T00:00:00Z"),
                lockUuid = null,
                createdAt = Instant.parse("2024-01-01T00:00:00Z"),
            )

        val inserted = adapter.insertOneRow(connection, row)

        assertTrue(inserted)
        val sql = capture.normalizedLast()
        assertTrue(sql.startsWith("IF NOT EXISTS"))
        assertTrue(sql.contains("INSERT INTO $tableName"))
    }

    @Test
    fun `selectForUpdateOneRow uses UPDLOCK and TOP 1`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.MsSqlServer, tableName)

        adapter.selectForUpdateOneRow(connection, "kind-1", "key-1")

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("SELECT TOP 1"))
        assertTrue(sql.contains("WITH (UPDLOCK)"))
        assertTrue(sql.contains("WHERE pKey = ? AND pKind = ?"))
    }

    @Test
    fun `selectFirstAvailableWithLock uses READPAST and TOP 1`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.MsSqlServer, tableName)

        adapter.selectFirstAvailableWithLock(connection, "kind-1", Instant.EPOCH)

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("SELECT TOP 1"))
        assertTrue(sql.contains("WITH (UPDLOCK, READPAST)"))
        assertTrue(sql.contains("ORDER BY scheduledAt"))
    }

    @Test
    fun `acquireManyOptimistically uses TOP and locking hints`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.MsSqlServer, tableName)

        adapter.acquireManyOptimistically(
            conn = connection,
            kind = "kind-1",
            limit = 5,
            lockUuid = "lock-uuid",
            timeout = Duration.ofSeconds(30),
            now = Instant.EPOCH,
        )

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("UPDATE $tableName"))
        assertTrue(sql.contains("SELECT TOP 5"))
        assertTrue(sql.contains("WITH (UPDLOCK, READPAST)"))
    }

    @Test
    fun `selectByKey uses TOP 1 without LIMIT`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.MsSqlServer, tableName)

        adapter.selectByKey(connection, "kind-1", "key-1")

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("SELECT TOP 1"))
        assertFalse(sql.contains("LIMIT"))
    }

    @Test
    fun `selectAllAvailableWithLock uses TOP without LIMIT`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.MsSqlServer, tableName)

        adapter.selectAllAvailableWithLock(connection, "lock-uuid", 10, null)

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("SELECT TOP 10"))
        assertTrue(sql.contains("ORDER BY id"))
        assertFalse(sql.contains("LIMIT"))
    }

    @Test
    fun `guardedUpdate uses exact timestamp matches`() {
        val capture = SqlCapture()
        val connection = capture.connection(executeUpdateResult = 1)
        val adapter = SQLVendorAdapter.create(JdbcDriver.HSQLDB, tableName)
        val currentRow =
            DBTableRow(
                pKey = "key-1",
                pKind = "kind-1",
                payload = "payload-1".toByteArray(),
                scheduledAt = Instant.parse("2024-01-01T00:00:00Z"),
                scheduledAtInitially = Instant.parse("2024-01-01T00:00:00Z"),
                lockUuid = "lock-1",
                createdAt = Instant.parse("2024-01-01T00:00:00Z"),
            )
        val updatedRow =
            DBTableRow(
                pKey = "key-1",
                pKind = "kind-1",
                payload = "payload-2".toByteArray(),
                scheduledAt = Instant.parse("2024-01-02T00:00:00Z"),
                scheduledAtInitially = Instant.parse("2024-01-01T00:00:00Z"),
                lockUuid = "lock-2",
                createdAt = Instant.parse("2024-01-01T00:00:00Z"),
            )

        adapter.guardedUpdate(connection, currentRow, updatedRow)

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("scheduledAtInitially = ?"))
        assertTrue(sql.contains("createdAt = ?"))
        assertFalse(sql.contains("scheduledAtInitially IN"))
        assertFalse(sql.contains("createdAt IN"))
    }

    @Test
    fun `deleteRowByFingerprint uses exact createdAt match`() {
        val capture = SqlCapture()
        val connection = capture.connection(executeUpdateResult = 1)
        val adapter = SQLVendorAdapter.create(JdbcDriver.HSQLDB, tableName)
        val row =
            DBTableRowWithId(
                id = 10L,
                data =
                    DBTableRow(
                        pKey = "key-1",
                        pKind = "kind-1",
                        payload = "payload-1".toByteArray(),
                        scheduledAt = Instant.parse("2024-01-01T00:00:00Z"),
                        scheduledAtInitially = Instant.parse("2024-01-01T00:00:00Z"),
                        lockUuid = "lock-1",
                        createdAt = Instant.parse("2024-01-01T00:00:00Z"),
                    ),
            )

        adapter.deleteRowByFingerprint(connection, row)

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("createdAt = ?"))
        assertFalse(sql.contains("createdAt IN"))
    }

    @Test
    fun `acquireRowByUpdate uses exact scheduledAt match`() {
        val capture = SqlCapture()
        val connection = capture.connection(executeUpdateResult = 1)
        val adapter = SQLVendorAdapter.create(JdbcDriver.HSQLDB, tableName)
        val row =
            DBTableRow(
                pKey = "key-1",
                pKind = "kind-1",
                payload = "payload-1".toByteArray(),
                scheduledAt = Instant.parse("2024-01-01T00:00:00Z"),
                scheduledAtInitially = Instant.parse("2024-01-01T00:00:00Z"),
                lockUuid = null,
                createdAt = Instant.parse("2024-01-01T00:00:00Z"),
            )

        adapter.acquireRowByUpdate(connection, row, "lock-1", Duration.ofSeconds(5), Instant.EPOCH)

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("scheduledAt = ?"))
        assertFalse(sql.contains("scheduledAt IN"))
    }

    private class SqlCapture {
        private val statements = mutableListOf<String>()

        fun connection(executeUpdateResult: Int = 0): Connection {
            val resultSet =
                Proxy.newProxyInstance(
                    ResultSet::class.java.classLoader,
                    arrayOf(ResultSet::class.java),
                    InvocationHandler { _, method, _ ->
                        when (method.name) {
                            "next" -> false
                            "close" -> null
                            else -> defaultReturn(method.returnType)
                        }
                    },
                ) as ResultSet

            val preparedStatement =
                Proxy.newProxyInstance(
                    PreparedStatement::class.java.classLoader,
                    arrayOf(PreparedStatement::class.java),
                    InvocationHandler { _, method, _ ->
                        when (method.name) {
                            "executeUpdate" -> executeUpdateResult
                            "executeQuery" -> resultSet
                            "executeBatch" -> IntArray(0)
                            "addBatch" -> null
                            "setString" -> null
                            "setBytes" -> null
                            "setTimestamp" -> null
                            "setNull" -> null
                            "close" -> null
                            else -> defaultReturn(method.returnType)
                        }
                    },
                ) as PreparedStatement

            return Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java),
                InvocationHandler { _, method, args ->
                    when (method.name) {
                        "prepareStatement" -> {
                            statements.add(args?.get(0) as String)
                            preparedStatement
                        }
                        "close" -> null
                        else -> defaultReturn(method.returnType)
                    }
                },
            ) as Connection
        }

        fun normalizedLast(): String = normalizeSql(statements.last())

        private fun normalizeSql(sql: String): String = sql.trim().replace(Regex("\\s+"), " ")

        private fun defaultReturn(returnType: Class<*>): Any? {
            return when {
                returnType == Boolean::class.javaPrimitiveType -> false
                returnType == Int::class.javaPrimitiveType -> 0
                returnType == Long::class.javaPrimitiveType -> 0L
                returnType == Double::class.javaPrimitiveType -> 0.0
                returnType == Float::class.javaPrimitiveType -> 0f
                returnType == Short::class.javaPrimitiveType -> 0.toShort()
                returnType == Byte::class.javaPrimitiveType -> 0.toByte()
                returnType == Void.TYPE -> null
                returnType == Timestamp::class.java -> Timestamp(0L)
                else -> null
            }
        }
    }
}
