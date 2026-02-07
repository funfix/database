package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.BatchUpdateException
import java.sql.SQLException
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLTransactionRollbackException
import java.sql.SQLTransientConnectionException
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.hsqldb.HSQLDBFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.mariadb.MariaDBFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.mssql.MSSQLFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.sqlite.SQLiteFilters
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
        fun `duplicateKey should not match generic SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("duplicate")
            assertFalse(HSQLDBFilters.duplicateKey.matches(ex))
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
        fun `duplicateKey should not match integrity constraint message`() {
            val ex = SQLException("INTEGRITY CONSTRAINT failed")
            assertFalse(HSQLDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should not match unrelated SQLException`() {
            val ex = SQLException("Some other error")
            assertFalse(HSQLDBFilters.duplicateKey.matches(ex))
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
        fun `duplicateKey should not match generic SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("constraint violation")
            assertFalse(MSSQLFilters.duplicateKey.matches(ex))
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
        fun `duplicateKey should not match generic SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("duplicate")
            assertFalse(SQLiteFilters.duplicateKey.matches(ex))
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
    }

    @Nested
    inner class MariaDBFiltersTest {
        @Test
        fun `transientFailure should match transient exceptions`() {
            val ex = SQLTransactionRollbackException("rollback")
            assertTrue(MariaDBFilters.transientFailure.matches(ex))
        }

        @Test
        fun `transientFailure should match MariaDB deadlock error code`() {
            val ex = SQLException("Deadlock found", "40001", 1213)
            assertTrue(MariaDBFilters.transientFailure.matches(ex))
        }

        @Test
        fun `transientFailure should match MariaDB lock wait timeout error code`() {
            val ex = SQLException("Lock wait timeout exceeded", "HY000", 1205)
            assertTrue(MariaDBFilters.transientFailure.matches(ex))
        }

        @Test
        fun `transientFailure should match MariaDB record changed error code`() {
            val ex = SQLException("Record has changed since last read", "HY000", 1020)
            assertTrue(MariaDBFilters.transientFailure.matches(ex))
        }

        @Test
        fun `transientFailure should match record changed in cause chain`() {
            val cause = SQLException("Record has changed since last read", "HY000", 1020)
            val wrapper = RuntimeException("batch failed", cause)
            assertTrue(MariaDBFilters.transientFailure.matches(wrapper))
        }

        @Test
        fun `transientFailure should match record changed in nextException chain`() {
            val next = SQLException("Record has changed since last read", "HY000", 1020)
            val batch = BatchUpdateException("batch failed", IntArray(0))
            batch.nextException = next
            assertTrue(MariaDBFilters.transientFailure.matches(batch))
        }

        @Test
        fun `duplicateKey should match MariaDB error code 1062`() {
            val ex = SQLException("Duplicate entry 'key1' for key 'PRIMARY'", "23000", 1062)
            assertTrue(MariaDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should not match generic SQLIntegrityConstraintViolationException`() {
            val ex = SQLIntegrityConstraintViolationException("constraint violation")
            assertFalse(MariaDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should match duplicate entry message`() {
            val ex = SQLException("Duplicate entry '123' for key 'PRIMARY'")
            assertTrue(MariaDBFilters.duplicateKey.matches(ex))
        }

        @Test
        fun `duplicateKey should not match unrelated SQLException`() {
            val ex = SQLException("Some other error")
            assertFalse(MariaDBFilters.duplicateKey.matches(ex))
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

        @Test
        fun `should return MariaDBFilters for MariaDB driver`() {
            val filters = filtersForDriver(JdbcDriver.MariaDB)
            assertTrue(filters === MariaDBFilters)
        }
    }
}
