package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

/**
 * Exception filters for MySQL-compatible databases (MySQL and MariaDB).
 *
 * Both MySQL and MariaDB share the same error codes and exception patterns, so they can use the
 * same filter implementation.
 */
internal object MySQLCompatibleFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    matchesTransient(e) -> true
                    matchesNextExceptionChain(e) -> true
                    else -> {
                        val cause = e.cause
                        cause != null && cause !== e && matches(cause)
                    }
                }
        }

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    e is SQLException && e.errorCode == 1062 && e.sqlState == "23000" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    private val DUPLICATE_KEY_KEYWORDS =
        listOf("duplicate entry", "primary key constraint", "unique constraint")

    private fun matchesTransient(e: Throwable): Boolean =
        when {
            CommonSqlFilters.transactionTransient.matches(e) -> true
            e is SQLException && e.errorCode == 1213 -> true // Deadlock
            e is SQLException && e.errorCode == 1205 -> true // Lock wait timeout
            e is SQLException && e.errorCode == 1020 -> true // Record changed since last read
            else -> false
        }

    private fun matchesNextExceptionChain(e: Throwable): Boolean {
        val sqlException = e as? SQLException ?: return false
        var next = sqlException.nextException
        while (next != null && next !== e) {
            if (matchesTransient(next)) {
                return true
            }
            next = next.nextException
        }
        return false
    }
}
