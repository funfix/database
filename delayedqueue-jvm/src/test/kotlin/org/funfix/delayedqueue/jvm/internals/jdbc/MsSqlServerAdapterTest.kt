package org.funfix.delayedqueue.jvm.internals.jdbc

import org.funfix.delayedqueue.jvm.JdbcDriver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class MsSqlServerAdapterTest {

    @Test
    fun `create returns MsSqlServerAdapter for MsSqlServer driver`() {
        val adapter = SQLVendorAdapter.create(JdbcDriver.MsSqlServer, "test_table")
        assertNotNull(adapter)
        assertEquals(JdbcDriver.MsSqlServer, adapter.driver)
    }

    @Test
    fun `create returns HSQLDBAdapter for HSQLDB driver`() {
        val adapter = SQLVendorAdapter.create(JdbcDriver.HSQLDB, "test_table")
        assertNotNull(adapter)
        assertEquals(JdbcDriver.HSQLDB, adapter.driver)
    }

    @Test
    fun `create throws for Sqlite driver`() {
        val ex =
            org.junit.jupiter.api.assertThrows<NotImplementedError> {
                SQLVendorAdapter.create(JdbcDriver.Sqlite, "test_table")
            }
        assertTrue(ex.message!!.contains("SQLite"))
    }
}

class MsSqlServerMigrationsTest {

    @Test
    fun `getMigrations returns non-empty list`() {
        val migrations = MsSqlServerMigrations.getMigrations("delayed_queue")
        assertTrue(migrations.isNotEmpty())
    }

    @Test
    fun `getMigrations returns single table creation migration`() {
        val migrations = MsSqlServerMigrations.getMigrations("delayed_queue")
        assertEquals(1, migrations.size)
    }

    @Nested
    inner class MigrationSqlTest {
        private val tableName = "my_queue"
        private val sql = MsSqlServerMigrations.getMigrations(tableName).first().sql

        @Test
        fun `sql contains CREATE TABLE`() {
            assertTrue(sql.contains("CREATE TABLE $tableName"), "Expected CREATE TABLE in SQL")
        }

        @Test
        fun `sql uses BIGINT IDENTITY for id column`() {
            assertTrue(
                sql.contains("BIGINT IDENTITY(1,1)"),
                "Expected BIGINT IDENTITY(1,1) for auto-increment",
            )
        }

        @Test
        fun `sql uses NVARCHAR for pKey`() {
            assertTrue(sql.contains("pKey NVARCHAR(200)"), "Expected NVARCHAR(200) for pKey")
        }

        @Test
        fun `sql uses NVARCHAR for pKind`() {
            assertTrue(sql.contains("pKind NVARCHAR(100)"), "Expected NVARCHAR(100) for pKind")
        }

        @Test
        fun `sql uses NVARCHAR MAX for payload`() {
            assertTrue(sql.contains("payload NVARCHAR(MAX)"), "Expected NVARCHAR(MAX) for payload")
        }

        @Test
        fun `sql uses DATETIMEOFFSET for scheduledAt`() {
            assertTrue(
                sql.contains("scheduledAt DATETIMEOFFSET"),
                "Expected DATETIMEOFFSET for scheduledAt",
            )
        }

        @Test
        fun `sql uses DATETIMEOFFSET for scheduledAtInitially`() {
            assertTrue(
                sql.contains("scheduledAtInitially DATETIMEOFFSET"),
                "Expected DATETIMEOFFSET for scheduledAtInitially",
            )
        }

        @Test
        fun `sql uses DATETIMEOFFSET for createdAt`() {
            assertTrue(
                sql.contains("createdAt DATETIMEOFFSET"),
                "Expected DATETIMEOFFSET for createdAt",
            )
        }

        @Test
        fun `sql uses VARCHAR 36 for lockUuid`() {
            assertTrue(sql.contains("lockUuid VARCHAR(36)"), "Expected VARCHAR(36) for lockUuid")
        }

        @Test
        fun `sql has composite primary key on pKey and pKind`() {
            assertTrue(
                sql.contains("PRIMARY KEY (pKey, pKind)"),
                "Expected composite PRIMARY KEY (pKey, pKind)",
            )
        }

        @Test
        fun `sql creates unique index on id`() {
            assertTrue(
                sql.contains("CREATE UNIQUE INDEX ${tableName}__IdUniqueIndex"),
                "Expected unique index on id",
            )
        }

        @Test
        fun `sql creates index on scheduledAt`() {
            assertTrue(
                sql.contains("CREATE INDEX ${tableName}__ScheduledAtIndex"),
                "Expected index on scheduledAt",
            )
        }

        @Test
        fun `sql creates composite index on pKind and scheduledAt`() {
            assertTrue(
                sql.contains("CREATE INDEX ${tableName}__KindPlusScheduledAtIndex"),
                "Expected composite index on pKind, scheduledAt",
            )
        }

        @Test
        fun `sql creates index on createdAt`() {
            assertTrue(
                sql.contains("CREATE INDEX ${tableName}__CreatedAtIndex"),
                "Expected index on createdAt",
            )
        }

        @Test
        fun `sql creates composite index on lockUuid and id`() {
            assertTrue(
                sql.contains("CREATE INDEX ${tableName}__LockUuidPlusIdIndex"),
                "Expected composite index on lockUuid, id",
            )
        }

        @Test
        fun `sql uses correct table name throughout`() {
            val differentTableName = "custom_delayed_queue"
            val customSql = MsSqlServerMigrations.getMigrations(differentTableName).first().sql

            assertTrue(
                customSql.contains("CREATE TABLE $differentTableName"),
                "Expected custom table name in CREATE TABLE",
            )
            assertTrue(
                customSql.contains("${differentTableName}__IdUniqueIndex"),
                "Expected custom table name in index names",
            )
        }
    }
}
