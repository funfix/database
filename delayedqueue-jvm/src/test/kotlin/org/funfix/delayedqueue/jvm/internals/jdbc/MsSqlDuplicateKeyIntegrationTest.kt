package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import javax.sql.DataSource
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.mssql.MSSQLFilters
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
import org.testcontainers.mssqlserver.MSSQLServerContainer
import org.testcontainers.utility.DockerImageName

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MsSqlDuplicateKeyIntegrationTest {
    private var container: MSSQLServerContainer? = null
    private lateinit var dataSource: DataSource

    @BeforeAll
    fun startContainer() {
        assumeTrue(
            DockerClientFactory.instance().isDockerAvailable,
            "Docker is not available; skipping MS-SQL tests",
        )
        val image =
            DockerImageName.parse("mcr.microsoft.com/azure-sql-edge:1.0.7")
                .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server")
        container = MSSQLServerContainer(image).acceptLicense().withPassword("StrongPassword!123")
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
                driver = JdbcDriver.MsSqlServer,
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
    fun `duplicateKey should match primary key violation from MS-SQL`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("IF OBJECT_ID('test', 'U') IS NOT NULL DROP TABLE test")
                    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, name NVARCHAR(50))")
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
        assertTrue(MSSQLFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should match unique key violation from MS-SQL`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("IF OBJECT_ID('test', 'U') IS NOT NULL DROP TABLE test")
                    stmt.execute(
                        "CREATE TABLE test (id INT PRIMARY KEY, email NVARCHAR(100) UNIQUE)"
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
        assertTrue(MSSQLFilters.duplicateKey.matches(ex!!))
    }

    @Test
    fun `duplicateKey should not match non unique constraint violation`() {
        val ex =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("IF OBJECT_ID('test', 'U') IS NOT NULL DROP TABLE test")
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
        assertFalse(MSSQLFilters.duplicateKey.matches(ex!!))
    }
}
