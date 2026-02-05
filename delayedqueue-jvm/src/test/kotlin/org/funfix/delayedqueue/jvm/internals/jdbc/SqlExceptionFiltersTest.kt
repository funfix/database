package org.funfix.delayedqueue.jvm.internals.jdbc

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.sql.SQLException
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLTransactionRollbackException
import java.sql.SQLTransientConnectionException

class SqlExceptionFiltersTest :
    FunSpec({
        context("CommonSqlFilters") {
            test("interrupted should match InterruptedException") {
                val ex = InterruptedException("test")
                CommonSqlFilters.interrupted.matches(ex) shouldBe true
            }

            test("interrupted should match InterruptedIOException") {
                val ex = java.io.InterruptedIOException("test")
                CommonSqlFilters.interrupted.matches(ex) shouldBe true
            }

            test("interrupted should match TimeoutException") {
                val ex = java.util.concurrent.TimeoutException("test")
                CommonSqlFilters.interrupted.matches(ex) shouldBe true
            }

            test("interrupted should match CancellationException") {
                val ex = java.util.concurrent.CancellationException("test")
                CommonSqlFilters.interrupted.matches(ex) shouldBe true
            }

            test("interrupted should find interruption in cause chain") {
                val rootCause = InterruptedException("root")
                val wrapped = RuntimeException("wrapper", rootCause)
                CommonSqlFilters.interrupted.matches(wrapped) shouldBe true
            }

            test("interrupted should not match regular exceptions") {
                val ex = RuntimeException("test")
                CommonSqlFilters.interrupted.matches(ex) shouldBe false
            }

            test("transactionTransient should match SQLTransactionRollbackException") {
                val ex = SQLTransactionRollbackException("deadlock")
                CommonSqlFilters.transactionTransient.matches(ex) shouldBe true
            }

            test("transactionTransient should match SQLTransientConnectionException") {
                val ex = SQLTransientConnectionException("connection lost")
                CommonSqlFilters.transactionTransient.matches(ex) shouldBe true
            }

            test("transactionTransient should not match generic SQLException") {
                val ex = SQLException("generic error")
                CommonSqlFilters.transactionTransient.matches(ex) shouldBe false
            }

            test("integrityConstraint should match SQLIntegrityConstraintViolationException") {
                val ex = SQLIntegrityConstraintViolationException("constraint violation")
                CommonSqlFilters.integrityConstraint.matches(ex) shouldBe true
            }

            test("integrityConstraint should not match generic SQLException") {
                val ex = SQLException("generic error")
                CommonSqlFilters.integrityConstraint.matches(ex) shouldBe false
            }
        }

        context("HSQLDBFilters") {
            test("transientFailure should match transient exceptions") {
                val ex = SQLTransactionRollbackException("rollback")
                HSQLDBFilters.transientFailure.matches(ex) shouldBe true
            }

            test("duplicateKey should match SQLIntegrityConstraintViolationException") {
                val ex = SQLIntegrityConstraintViolationException("duplicate")
                HSQLDBFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should match HSQLDB error code") {
                val ex = SQLException("duplicate", "23505", -104)
                HSQLDBFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should match primary key constraint message") {
                val ex = SQLException("Violation of PRIMARY KEY constraint")
                HSQLDBFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should match unique constraint message") {
                val ex = SQLException("UNIQUE constraint violation")
                HSQLDBFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should match integrity constraint message") {
                val ex = SQLException("INTEGRITY CONSTRAINT failed")
                HSQLDBFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should not match unrelated SQLException") {
                val ex = SQLException("Some other error")
                HSQLDBFilters.duplicateKey.matches(ex) shouldBe false
            }

            test("invalidTable should match message") {
                val ex = SQLException("invalid object name 'my_table'")
                HSQLDBFilters.invalidTable.matches(ex) shouldBe true
            }

            test("invalidTable should not match other exceptions") {
                val ex = SQLException("other error")
                HSQLDBFilters.invalidTable.matches(ex) shouldBe false
            }

            test("objectAlreadyExists should not match for HSQLDB") {
                val ex = SQLException("object exists")
                HSQLDBFilters.objectAlreadyExists.matches(ex) shouldBe false
            }
        }

        context("MSSQLFilters") {
            test("transientFailure should match transient exceptions") {
                val ex = SQLTransactionRollbackException("rollback")
                MSSQLFilters.transientFailure.matches(ex) shouldBe true
            }

            test("duplicateKey should match primary key error code") {
                val ex = SQLException("primary key violation", "23000", 2627)
                MSSQLFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should match unique key error code") {
                val ex = SQLException("unique key violation", "23000", 2601)
                MSSQLFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should match constraint violation message") {
                val ex = SQLException("Violation of PRIMARY KEY constraint 'PK_Test'")
                MSSQLFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("duplicateKey should match SQLIntegrityConstraintViolationException") {
                val ex = SQLIntegrityConstraintViolationException("constraint violation")
                MSSQLFilters.duplicateKey.matches(ex) shouldBe true
            }

            test("invalidTable should match MSSQL error code") {
                val ex = SQLException("Invalid object name", "42S02", 208)
                MSSQLFilters.invalidTable.matches(ex) shouldBe true
            }

            test("invalidTable should match message") {
                val ex = SQLException("Invalid object name 'dbo.MyTable'")
                MSSQLFilters.invalidTable.matches(ex) shouldBe true
            }

            test("failedToResumeTransaction should match SQL Server message") {
                val ex = SQLException("The server failed to resume the transaction")
                MSSQLFilters.failedToResumeTransaction.matches(ex) shouldBe false
            }
        }
    })
