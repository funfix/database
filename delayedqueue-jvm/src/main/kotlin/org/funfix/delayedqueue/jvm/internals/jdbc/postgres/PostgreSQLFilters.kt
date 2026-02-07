package org.funfix.delayedqueue.jvm.internals.jdbc.postgres

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

/** PostgreSQL-specific exception filters. */
internal object PostgreSQLFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.transactionTransient.matches(e) -> true
                    // PostgreSQL serialization failure (40001) or deadlock detected (40P01)
                    e is SQLException && e.sqlState in setOf("40001", "40P01") -> true
                    else -> false
                }
        }

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.integrityConstraint.matches(e) -> true
                    // PostgreSQL unique_violation (23505)
                    e is SQLException && e.sqlState == "23505" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    override val invalidTable: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                // PostgreSQL undefined_table (42P01)
                e is SQLException && e.sqlState == "42P01"
        }

    override val objectAlreadyExists: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                // PostgreSQL duplicate_table (42P07) or duplicate_object (42710)
                e is SQLException && e.sqlState in setOf("42P07", "42710")
        }

    private val DUPLICATE_KEY_KEYWORDS =
        listOf("primary key constraint", "unique constraint", "duplicate key")
}
