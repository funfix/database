/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.jvm.internals.utils

import java.sql.SQLException
import javax.sql.DataSource
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.ConnectionPool
import org.funfix.delayedqueue.jvm.internals.jdbc.Database
import org.funfix.delayedqueue.jvm.internals.jdbc.SafeConnection
import org.funfix.delayedqueue.jvm.internals.jdbc.execute
import org.funfix.delayedqueue.jvm.internals.jdbc.query
import org.funfix.delayedqueue.jvm.internals.jdbc.withConnection
import org.funfix.delayedqueue.jvm.internals.jdbc.withTransaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DatabaseTests {
    private lateinit var config: JdbcConnectionConfig
    private lateinit var dataSource: DataSource
    private lateinit var database: Database

    @BeforeEach
    fun setUp() {
        config = JdbcConnectionConfig(url = "jdbc:sqlite::memory:", driver = JdbcDriver.Sqlite)
        dataSource = ConnectionPool.createDataSource(config)
        database = Database(dataSource, JdbcDriver.Sqlite, dataSource as AutoCloseable)
    }

    @AfterEach
    fun tearDown() {
        (dataSource as? AutoCloseable)?.close()
    }

    @Test
    fun `buildHikariConfig sets correct values`() {
        val hikariConfig = ConnectionPool.buildHikariConfig(config)
        assertEquals(config.url, hikariConfig.jdbcUrl)
        assertEquals(config.driver.className, hikariConfig.driverClassName)
    }

    @Test
    fun `createDataSource returns working DataSource`() {
        dataSource.connection.use { conn ->
            assertFalse(conn.isClosed)
            assertTrue(conn.metaData.driverName.contains("SQLite", ignoreCase = true))
        }
    }

    @Test
    fun `Database withConnection executes block and closes connection`() {
        var connectionClosedAfter: Boolean
        var connectionRef: SafeConnection? = null
        val result =
            database.withConnection { safeConn ->
                connectionRef = safeConn
                safeConn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
                "done"
            }
        // Connection is closed after block
        connectionClosedAfter = connectionRef?.underlying?.isClosed ?: false
        assertEquals("done", result)
        assertTrue(connectionClosedAfter)
    }

    @Test
    fun `Database withTransaction commits on success`() {
        database.withConnection { safeConn ->
            safeConn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        }
        database.withTransaction { safeConn ->
            safeConn.execute("INSERT INTO test (name) VALUES ('foo')")
        }
        val count =
            database.withConnection { safeConn ->
                safeConn.query("SELECT COUNT(*) FROM test") { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        assertEquals(1, count)
    }

    @Test
    fun `Database withTransaction rolls back on exception`() {
        database.withConnection { safeConn ->
            safeConn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        }
        assertThrows(SQLException::class.java) {
            database.withTransaction { safeConn ->
                safeConn.execute("INSERT INTO test (name) VALUES ('foo')")
                // This will fail (duplicate primary key)
                safeConn.execute("INSERT INTO test (id, name) VALUES (1, 'bar')")
                safeConn.execute("INSERT INTO test (id, name) VALUES (1, 'baz')")
            }
        }
        val count =
            database.withConnection { safeConn ->
                safeConn.query("SELECT COUNT(*) FROM test") { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        assertEquals(0, count)
    }

    @Test
    fun `Statement query executes block and returns result`() {
        database.withConnection { safeConn ->
            safeConn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            safeConn.execute("INSERT INTO test (name) VALUES ('foo')")
            val result =
                safeConn.query("SELECT name FROM test") { rs ->
                    rs.next()
                    rs.getString(1)
                }
            assertEquals("foo", result)
        }
    }
}
