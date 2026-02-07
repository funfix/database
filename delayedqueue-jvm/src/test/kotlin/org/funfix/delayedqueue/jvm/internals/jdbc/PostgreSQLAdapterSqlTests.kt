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

class PostgreSQLAdapterSqlTests {
    private val tableName = "dq_table"

    @Test
    fun `insert uses ON CONFLICT DO NOTHING pattern`() {
        val capture = SqlCapture()
        val connection = capture.connection(executeUpdateResult = 1)
        val adapter = SQLVendorAdapter.create(JdbcDriver.PostgreSQL, tableName)
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
        assertTrue(sql.contains("INSERT INTO $tableName"))
        assertTrue(sql.contains("ON CONFLICT (pKey, pKind) DO NOTHING"))
    }

    @Test
    fun `insert returns false when row already exists`() {
        val capture = SqlCapture()
        val connection = capture.connection(executeUpdateResult = 0)
        val adapter = SQLVendorAdapter.create(JdbcDriver.PostgreSQL, tableName)
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

        assertFalse(inserted)
    }

    @Test
    fun `selectForUpdateOneRow uses FOR UPDATE and LIMIT 1`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.PostgreSQL, tableName)

        adapter.selectForUpdateOneRow(connection, "kind-1", "key-1")

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("LIMIT 1"))
        assertTrue(sql.contains("FOR UPDATE"))
        assertTrue(sql.contains("WHERE pKey = ? AND pKind = ?"))
        assertFalse(sql.contains("SKIP LOCKED"))
    }

    @Test
    fun `selectFirstAvailableWithLock uses FOR UPDATE SKIP LOCKED`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.PostgreSQL, tableName)

        adapter.selectFirstAvailableWithLock(connection, "kind-1", Instant.EPOCH)

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("LIMIT 1"))
        assertTrue(sql.contains("FOR UPDATE SKIP LOCKED"))
        assertTrue(sql.contains("ORDER BY scheduledAt"))
    }

    @Test
    fun `acquireManyOptimistically uses LIMIT and FOR UPDATE SKIP LOCKED`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.PostgreSQL, tableName)

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
        assertTrue(sql.contains("LIMIT 5"))
        assertTrue(sql.contains("FOR UPDATE SKIP LOCKED"))
    }

    @Test
    fun `selectByKey uses LIMIT 1`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.PostgreSQL, tableName)

        adapter.selectByKey(connection, "kind-1", "key-1")

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("LIMIT 1"))
        assertFalse(sql.contains("TOP"))
    }

    @Test
    fun `selectAllAvailableWithLock uses LIMIT`() {
        val capture = SqlCapture()
        val connection = capture.connection()
        val adapter = SQLVendorAdapter.create(JdbcDriver.PostgreSQL, tableName)

        adapter.selectAllAvailableWithLock(connection, "lock-uuid", 10, null)

        val sql = capture.normalizedLast()
        assertTrue(sql.contains("LIMIT 10"))
        assertTrue(sql.contains("ORDER BY id"))
        assertFalse(sql.contains("TOP"))
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
                            "setLong" -> null
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
