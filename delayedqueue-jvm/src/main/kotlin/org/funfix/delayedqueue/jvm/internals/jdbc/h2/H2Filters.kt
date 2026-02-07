package org.funfix.delayedqueue.jvm.internals.jdbc.h2

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

/** H2-specific exception filters. */
internal object H2Filters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter = CommonSqlFilters.transactionTransient

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    e is SQLException && e.errorCode == 23505 && e.sqlState == "23505" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    private val DUPLICATE_KEY_KEYWORDS =
        listOf("unique index or primary key", "unique constraint", "unique index")
}
