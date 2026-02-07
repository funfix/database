package org.funfix.delayedqueue.jvm.internals.jdbc.mssql

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

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
