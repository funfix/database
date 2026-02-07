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

package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import javax.sql.DataSource
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.oracle.OracleFilters
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.OracleContainer
import org.testcontainers.utility.DockerImageName

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OracleDuplicateKeyIntegrationTest {
    private var container: OracleContainer? = null
    private lateinit var dataSource: DataSource

    @BeforeAll
    fun startContainer() {
        assumeTrue(
            DockerClientFactory.instance().isDockerAvailable,
            "Docker is not available; skipping Oracle tests",
        )
        container =
            OracleContainer(
                    DockerImageName.parse("gvenzl/oracle-free:23.4-slim")
                        .asCompatibleSubstituteFor("gvenzl/oracle-xe")
                )
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test")
        container?.start()
    }

    @AfterAll
    fun stopContainer() {
        container?.stop()
    }

    @BeforeEach
    fun setUp() {
        val c = container ?: return
        val config =
            JdbcConnectionConfig(
                url = c.jdbcUrl,
                driver = JdbcDriver.Oracle,
                username = c.username,
                password = c.password,
            )
        dataSource = ConnectionPool.createDataSource(config)
    }

    @AfterEach
    fun tearDown() {
        (dataSource as? AutoCloseable)?.close()
    }

    @Test
    fun `duplicateKey should match primary key violation from Oracle`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    dropTableIfExists(stmt)
                    stmt.execute("CREATE TABLE test (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
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
        assertTrue(OracleFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should match unique key violation from Oracle`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    dropTableIfExists(stmt)
                    stmt.execute(
                        "CREATE TABLE test (id NUMBER PRIMARY KEY, email VARCHAR2(50) UNIQUE)"
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
        assertTrue(OracleFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should not match non unique constraint violation`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    dropTableIfExists(stmt)
                    stmt.execute(
                        "CREATE TABLE test (id NUMBER PRIMARY KEY, value NUMBER CHECK (value > 0))"
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
        assertFalse(OracleFilters.duplicateKey.matches(ex!!))
    }

    private fun dropTableIfExists(statement: java.sql.Statement) {
        statement.execute(
            """
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE test PURGE';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """
                .trimIndent()
        )
    }
}
