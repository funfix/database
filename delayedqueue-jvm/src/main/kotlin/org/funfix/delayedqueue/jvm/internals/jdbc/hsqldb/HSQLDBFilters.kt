package org.funfix.delayedqueue.jvm.internals.jdbc.hsqldb

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

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
