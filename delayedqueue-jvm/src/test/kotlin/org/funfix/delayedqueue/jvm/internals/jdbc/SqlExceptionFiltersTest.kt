package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLTransactionRollbackException
import java.sql.SQLTransientConnectionException
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
}
