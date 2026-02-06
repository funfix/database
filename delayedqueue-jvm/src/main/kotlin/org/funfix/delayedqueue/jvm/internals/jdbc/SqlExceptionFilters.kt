package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLTransactionRollbackException
import java.sql.SQLTransientConnectionException
import org.funfix.delayedqueue.jvm.JdbcDriver

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
    val invalidTable: SqlExceptionFilter
    val objectAlreadyExists: SqlExceptionFilter
}

/** HSQLDB-specific exception filters. */
internal object HSQLDBFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter = CommonSqlFilters.transactionTransient

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.integrityConstraint.matches(e) -> true
                    e is SQLException && e.errorCode == -104 && e.sqlState == "23505" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    override val invalidTable: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                e is SQLException && matchesMessage(e.message, listOf("invalid object name"))
        }

    override val objectAlreadyExists: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean = false
        }

    private val DUPLICATE_KEY_KEYWORDS =
        listOf("primary key constraint", "unique constraint", "integrity constraint")
}

/** Microsoft SQL Server-specific exception filters. */
internal object MSSQLFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.transactionTransient.matches(e) -> true
                    e is SQLException && hasSQLServerError(e, 1205) -> true // Deadlock
                    failedToResumeTransaction.matches(e) -> true
                    else -> false
                }
        }

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.integrityConstraint.matches(e) -> true
                    e is SQLException && hasSQLServerError(e, 2627, 2601) -> true
                    e is SQLException &&
                        e.errorCode in setOf(2627, 2601) &&
                        e.sqlState == "23000" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    override val invalidTable: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    e is SQLException && e.errorCode == 208 && e.sqlState == "42S02" -> true
                    e is SQLException && matchesMessage(e.message, listOf("invalid object name")) ->
                        true
                    else -> false
                }
        }

    override val objectAlreadyExists: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                e is SQLException && hasSQLServerError(e, 2714, 2705, 1913, 15248, 15335)
        }

    val failedToResumeTransaction: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                isSQLServerException(e) &&
                    e.message?.contains("The server failed to resume the transaction") == true
        }

    private val DUPLICATE_KEY_KEYWORDS =
        listOf("primary key constraint", "unique constraint", "integrity constraint")
}

private fun matchesMessage(message: String?, keywords: List<String>): Boolean {
    if (message == null) return false
    val lowerMessage = message.lowercase()
    return keywords.any { lowerMessage.contains(it.lowercase()) }
}

private fun hasSQLServerError(e: Throwable, vararg errorNumbers: Int): Boolean {
    if (!isSQLServerException(e)) return false

    return try {
        val sqlServerErrorMethod = e.javaClass.getMethod("getSQLServerError")
        val sqlServerError = sqlServerErrorMethod.invoke(e)

        if (sqlServerError != null) {
            val getErrorNumberMethod = sqlServerError.javaClass.getMethod("getErrorNumber")
            val errorNumber = getErrorNumberMethod.invoke(sqlServerError) as? Int
            errorNumber != null && errorNumber in errorNumbers
        } else {
            false
        }
    } catch (_: Exception) {
        false
    }
}

private fun isSQLServerException(e: Throwable): Boolean =
    e.javaClass.name == "com.microsoft.sqlserver.jdbc.SQLServerException"

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
        JdbcDriver.MsSqlServer -> MSSQLFilters
        JdbcDriver.Sqlite -> HSQLDBFilters // Use HSQLDB filters as baseline
    }
