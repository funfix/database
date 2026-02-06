package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLTransactionRollbackException
import java.sql.SQLTransientConnectionException
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class SqlExceptionFiltersTest {

    @Nested
    inner class CommonSqlFiltersTest {
        @Test
        fun `interrupted should match InterruptedException`() {
            val ex = InterruptedException("test")
            assertTrue(CommonSqlFilters.interrupted.matches(ex))
        }

        @Test
        fun `interrupted should match InterruptedIOException`() {
            val ex = java.io.InterruptedIOException("test")
            assertTrue(CommonSqlFilters.interrupted.matches(ex))
        }

        @Test
        fun `interrupted should match TimeoutException`() {
            val ex = java.util.concurrent.TimeoutException("test")
            assertTrue(CommonSqlFilters.interrupted.matches(ex))
        }

        @Test
        fun `interrupted should match CancellationException`() {
            val ex = java.util.concurrent.CancellationException("test")
            assertTrue(CommonSqlFilters.interrupted.matches(ex))
        }

        @Test
        fun `interrupted should find interruption in cause chain`() {
            val rootCause = InterruptedException("root")
            val wrapped = RuntimeException("wrapper", rootCause)
            assertTrue(CommonSqlFilters.interrupted.matches(wrapped))
        }

        @Test
        fun `interrupted should not match regular exceptions`() {
            val ex = RuntimeException("test")
            assertFalse(CommonSqlFilters.interrupted.matches(ex))
        }

        @Test
        fun `transactionTransient should match SQLTransactionRollbackException`() {
            val ex = SQLTransactionRollbackException("deadlock")
            assertTrue(CommonSqlFilters.transactionTransient.matches(ex))
        }

        @Test
        fun `transactionTransient should match SQLTransientConnectionException`() {
            val ex = SQLTransientConnectionException("connection lost")
            assertTrue(CommonSqlFilters.transactionTransient.matches(ex))
        }

        @Test
        fun `transactionTransient should not match generic SQLException`() {
            val ex = SQLException("generic error")
            assertFalse(CommonSqlFilters.transactionTransient.matches(ex))
        }

        @Test
        fun `integrityConstraint should match SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("constraint violation")
            assertTrue(CommonSqlFilters.integrityConstraint.matches(ex))
        }

        @Test
        fun `integrityConstraint should not match generic SQLException`() {
            val ex = SQLException("generic error")
            assertFalse(CommonSqlFilters.integrityConstraint.matches(ex))
        }
    }

    @Nested
    inner class HSQLDBFiltersTest {
        @Test
        fun `transientFailure should match transient exceptions`() {
            val ex = SQLTransactionRollbackException("rollback")
            assertTrue(HSQLDBFilters.transientFailure.matches(ex))
        }

        @Test
        fun `duplicateKey should match SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("duplicate")
            assertTrue(HSQLDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match HSQLDB error code`() {
            val ex = SQLException("duplicate", "23505", -104)
            assertTrue(HSQLDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match primary key constraint message`() {
            val ex = SQLException("Violation of PRIMARY KEY constraint")
            assertTrue(HSQLDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match unique constraint message`() {
            val ex = SQLException("UNIQUE constraint violation")
            assertTrue(HSQLDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match integrity constraint message`() {
            val ex = SQLException("INTEGRITY CONSTRAINT failed")
            assertTrue(HSQLDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should not match unrelated SQLException`() {
            val ex = SQLException("Some other error")
            assertFalse(HSQLDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `invalidTable should match message`() {
            val ex = SQLException("invalid object name 'my_table'")
            assertTrue(HSQLDBFilters.invalidTable.matches(ex))
        }

        @Test
        fun `invalidTable should not match other exceptions`() {
            val ex = SQLException("other error")
            assertFalse(HSQLDBFilters.invalidTable.matches(ex))
        }

        @Test
        fun `objectAlreadyExists should not match for HSQLDB`() {
            val ex = SQLException("object exists")
            assertFalse(HSQLDBFilters.objectAlreadyExists.matches(ex))
        }
    }

    @Nested
    inner class MSSQLFiltersTest {
        @Test
        fun `transientFailure should match transient exceptions`() {
            val ex = SQLTransactionRollbackException("rollback")
            assertTrue(MSSQLFilters.transientFailure.matches(ex))
        }

        @Test
        fun `duplicateKey should match primary key error code`() {
            val ex = SQLException("primary key violation", "23000", 2627)
            assertTrue(MSSQLFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match unique key error code`() {
            val ex = SQLException("unique key violation", "23000", 2601)
            assertTrue(MSSQLFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match constraint violation message`() {
            val ex = SQLException("Violation of PRIMARY KEY constraint 'PK_Test'")
            assertTrue(MSSQLFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("constraint violation")
            assertTrue(MSSQLFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `invalidTable should match MSSQL error code`() {
            val ex = SQLException("Invalid object name", "42S02", 208)
            assertTrue(MSSQLFilters.invalidTable.matches(ex))
        }

        @Test
        fun `invalidTable should match message`() {
            val ex = SQLException("Invalid object name 'dbo.MyTable'")
            assertTrue(MSSQLFilters.invalidTable.matches(ex))
        }

        @Test
        fun `failedToResumeTransaction should match SQL Server message`() {
            val ex = SQLException("The server failed to resume the transaction")
            assertFalse(MSSQLFilters.failedToResumeTransaction.matches(ex))
        }
    }

    @Nested
    inner class SQLiteFiltersTest {
        @Test
        fun `transientFailure should match transient exceptions`() {
            val ex = SQLTransactionRollbackException("rollback")
            assertTrue(SQLiteFilters.transientFailure.matches(ex))
        }

        @Test
        fun `transientFailure should not match generic SQLException`() {
            val ex = SQLException("generic error")
            assertFalse(SQLiteFilters.transientFailure.matches(ex))
        }

        @Test
        fun `duplicateKey should match SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("duplicate")
            assertTrue(SQLiteFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match unique constraint message`() {
            val ex = SQLException("UNIQUE constraint failed: table.pKey")
            assertTrue(SQLiteFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match primary key constraint message`() {
            val ex = SQLException("PRIMARY KEY CONSTRAINT violation")
            assertTrue(SQLiteFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should not match unrelated SQLException`() {
            val ex = SQLException("Some other error")
            assertFalse(SQLiteFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `invalidTable should match no such table message`() {
            val ex =
                SQLException(
                    "[SQLITE_ERROR] SQL error or missing database (no such table: my_table)"
                )
            assertTrue(SQLiteFilters.invalidTable.matches(ex))
        }

        @Test
        fun `invalidTable should not match other exceptions`() {
            val ex = SQLException("other error")
            assertFalse(SQLiteFilters.invalidTable.matches(ex))
        }

        @Test
        fun `objectAlreadyExists should match already exists message`() {
            val ex = SQLException("table my_table already exists")
            assertTrue(SQLiteFilters.objectAlreadyExists.matches(ex))
        }

        @Test
        fun `objectAlreadyExists should not match other exceptions`() {
            val ex = SQLException("other error")
            assertFalse(SQLiteFilters.objectAlreadyExists.matches(ex))
        }
    }

    @Nested
    inner class FiltersForDriverTest {
        @Test
        fun `should return HSQLDBFilters for HSQLDB driver`() {
            val filters = filtersForDriver(JdbcDriver.HSQLDB)
            assertTrue(filters === HSQLDBFilters)
        }

        @Test
        fun `should return MSSQLFilters for MsSqlServer driver`() {
            val filters = filtersForDriver(JdbcDriver.MsSqlServer)
            assertTrue(filters === MSSQLFilters)
        }

        @Test
        fun `should return SQLiteFilters for Sqlite driver`() {
            val filters = filtersForDriver(JdbcDriver.Sqlite)
            assertTrue(filters === SQLiteFilters)
        }
    }
}
