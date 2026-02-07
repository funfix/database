package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import javax.sql.DataSource
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.sqlite.SQLiteFilters
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SqliteDuplicateKeyIntegrationTest {
    private lateinit var dataSource: DataSource

    @BeforeEach
    fun setUp() {
        val config = JdbcConnectionConfig(url = "jdbc:sqlite::memory:", driver = JdbcDriver.Sqlite)
        dataSource = ConnectionPool.createDataSource(config)
    }

    @AfterEach
    fun tearDown() {
        (dataSource as? AutoCloseable)?.close()
    }

    @Test
    fun `duplicateKey should match primary key violation from SQLite`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
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
        assertTrue(SQLiteFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should match unique key violation from SQLite`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
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
        assertTrue(SQLiteFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should not match non unique constraint violation`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER CHECK (value > 0))"
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
        assertFalse(SQLiteFilters.duplicateKey.matches(ex!!))
    }
}
