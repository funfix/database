package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import java.util.UUID
import javax.sql.DataSource
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.hsqldb.HSQLDBFilters
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class HsqlDbDuplicateKeyIntegrationTest {
    private lateinit var dataSource: DataSource

    @BeforeEach
    fun setUp() {
        val dbName = "dupkeys_${UUID.randomUUID()}"
        val config =
            JdbcConnectionConfig(url = "jdbc:hsqldb:mem:$dbName", driver = JdbcDriver.HSQLDB)
        dataSource = ConnectionPool.createDataSource(config)
    }

    @AfterEach
    fun tearDown() {
        (dataSource as? AutoCloseable)?.close()
    }

    @Test
    fun `duplicateKey should match primary key violation from HSQLDB`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS test")
                    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50))")
                    stmt.execute("INSERT INTO test (id, name) VALUES (1, 'first')")
                    try {
                        stmt.execute("INSERT INTO test (id, name) VALUES (1, 'second')")
                        null
                    } catch (e: SQLException) {
                        e
                    }
                }
            }

        assertNotNull(ex)
        assertTrue(HSQLDBFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should match unique key violation from HSQLDB`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS test")
                    stmt.execute(
                        "CREATE TABLE test (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE)"
                    )
                    stmt.execute("INSERT INTO test (id, email) VALUES (1, 'a@b.com')")
                    try {
                        stmt.execute("INSERT INTO test (id, email) VALUES (2, 'a@b.com')")
                        null
                    } catch (e: SQLException) {
                        e
                    }
                }
            }

        assertNotNull(ex)
        assertTrue(HSQLDBFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should not match non unique constraint violation`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS test")
                    stmt.execute(
                        "CREATE TABLE test (id INT PRIMARY KEY, value INT CHECK (value > 0))"
                    )
                    try {
                        stmt.execute("INSERT INTO test (id, value) VALUES (1, 0)")
                        null
                    } catch (e: SQLException) {
                        e
                    }
                }
            }

        assertNotNull(ex)
        assertFalse(HSQLDBFilters.duplicateKey.matches(ex!!))
    }
}
