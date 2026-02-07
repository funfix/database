package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLTransactionRollbackException
import java.sql.SQLTransientConnectionException
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.h2.H2Filters
import org.funfix.delayedqueue.jvm.internals.jdbc.hsqldb.HSQLDBFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.mariadb.MariaDBFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.mssql.MSSQLFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.mysql.MySQLFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.oracle.OracleFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.postgres.PostgreSQLFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.sqlite.SQLiteFilters

/**
 * Filter for matching SQL exceptions based on specific criteria. Designed for extensibility across
 * different RDBMS vendors.
 */
internal interface SqlExceptionFilter {
    fun matches(e: Throwable): Boolean
}

/** Common SQL exception filters that work across databases. */
internal object CommonSqlFilters {
    /** Matches interruption-related exceptions. */
    val interrupted: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when (e) {
                    is InterruptedException -> true
                    is java.io.InterruptedIOException -> true
                    is java.nio.channels.InterruptedByTimeoutException -> true
                    is java.util.concurrent.CancellationException -> true
                    is java.util.concurrent.TimeoutException -> true
                    else -> {
                        val cause = e.cause
                        cause != null && cause !== e && matches(cause)
                    }
                }
        }

    /** Matches transaction rollback and transient connection exceptions. */
    val transactionTransient: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                e is SQLTransactionRollbackException || e is SQLTransientConnectionException
        }

    /** Matches integrity constraint violations (standard JDBC). */
    val integrityConstraint: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                e is SQLIntegrityConstraintViolationException
        }
}

/** RDBMS-specific exception filters for different database vendors. */
internal interface RdbmsExceptionFilters {
    val transientFailure: SqlExceptionFilter
    val duplicateKey: SqlExceptionFilter
}

internal fun matchesMessage(message: String?, keywords: List<String>): Boolean {
    if (message == null) return false
    val lowerMessage = message.lowercase()
    return keywords.any { lowerMessage.contains(it.lowercase()) }
}

/**
 * Maps a JDBC driver to its corresponding exception filters.
 *
 * This ensures that exception matching behavior is consistent with the database vendor. For
 * example, HSQLDB and MS SQL Server have different error codes for duplicate keys.
 *
 * @param driver the JDBC driver
 * @return the appropriate exception filters for that driver
 */
internal fun filtersForDriver(driver: JdbcDriver): RdbmsExceptionFilters =
    when (driver) {
        JdbcDriver.HSQLDB -> HSQLDBFilters
        JdbcDriver.H2 -> H2Filters
        JdbcDriver.MsSqlServer -> MSSQLFilters
        JdbcDriver.Sqlite -> SQLiteFilters
        JdbcDriver.MariaDB -> MariaDBFilters
        JdbcDriver.MySQL -> MySQLFilters
        JdbcDriver.PostgreSQL -> PostgreSQLFilters
        JdbcDriver.Oracle -> OracleFilters
        else -> throw IllegalArgumentException("Unsupported JDBC driver: ${driver.className}")
    }
