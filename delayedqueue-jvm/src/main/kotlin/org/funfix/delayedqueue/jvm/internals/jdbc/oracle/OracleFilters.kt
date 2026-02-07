package org.funfix.delayedqueue.jvm.internals.jdbc.oracle

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

/** Oracle-specific exception filters. */
internal object OracleFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.transactionTransient.matches(e) -> true
                    // ORA-00060 (deadlock), ORA-00054 (resource busy), ORA-08177 (serialization)
                    e is SQLException && e.errorCode in setOf(60, 54, 8177) -> true
                    // SQLSTATE 40001 (serialization failure)
                    e is SQLException && e.sqlState == "40001" -> true
                    else -> false
                }
        }

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    // ORA-00001: unique constraint violated
                    e is SQLException && e.errorCode == 1 -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    private val DUPLICATE_KEY_KEYWORDS = listOf("unique constraint", "duplicate key")
}
