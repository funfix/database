package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

/** MariaDB-specific exception filters. */
internal object MariaDBFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.transactionTransient.matches(e) -> true
                    e is SQLException && e.errorCode == 1213 -> true // Deadlock
                    e is SQLException && e.errorCode == 1205 -> true // Lock wait timeout
                    e is SQLException && e.errorCode == 1020 ->
                        true // Record changed since last read
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
                    CommonSqlFilters.integrityConstraint.matches(e) -> true
                    e is SQLException && e.errorCode == 1062 && e.sqlState == "23000" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    override val invalidTable: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    e is SQLException && e.errorCode == 1146 && e.sqlState == "42S02" -> true
                    e is SQLException &&
                        matchesMessage(e.message, listOf("doesn't exist", "table")) -> true
                    else -> false
                }
        }

    override val objectAlreadyExists: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                e is SQLException && e.errorCode == 1050 // Table already exists
        }

    private val DUPLICATE_KEY_KEYWORDS =
        listOf("duplicate entry", "primary key constraint", "unique constraint")
}
