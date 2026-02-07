package org.funfix.delayedqueue.jvm.internals.jdbc.sqlite

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

/** SQLite-specific exception filters. */
internal object SQLiteFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.transactionTransient.matches(e) -> true
                    // SQLite BUSY (error code 5) — database is locked by another connection
                    e is SQLException && isSQLiteBaseCode(e, 5) -> true
                    // SQLite LOCKED (error code 6) — table-level lock within a connection
                    e is SQLException && isSQLiteBaseCode(e, 6) -> true
                    else -> false
                }
        }

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    CommonSqlFilters.integrityConstraint.matches(e) -> true
                    // SQLite CONSTRAINT_PRIMARYKEY (2067) and CONSTRAINT_UNIQUE (2579)
                    e is SQLException && isSQLiteResultCode(e, 2067, 2579) -> true
                    // SQLite generic CONSTRAINT (19) when paired with duplicate-key text
                    e is SQLException &&
                        isSQLiteResultCode(e, 19) &&
                        matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    // If the message text indicates a duplicate-key violation, accept it
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    override val invalidTable: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                e is SQLException && matchesMessage(e.message, listOf("no such table"))
        }

    override val objectAlreadyExists: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                e is SQLException && matchesMessage(e.message, listOf("already exists"))
        }

    private val DUPLICATE_KEY_KEYWORDS = listOf("primary key constraint", "unique constraint")
}

/**
 * Attempts to detect SQLite-specific result codes in exceptions in a defensive way.
 *
 * Strategy:
 * 1. Walk the cause chain searching for a throwable whose class name contains "SQLiteException";
 * 2. Try JDBC-standard accessors first: if the throwable is a SQLException, use getErrorCode();
 * 3. As a last resort, attempt reflection on well-known driver methods (getResultCode -> code) but
 *    be defensive and return false on any reflection failure.
 */
private fun isSQLiteResultCode(e: Throwable, vararg codes: Int): Boolean {
    val code = sqliteResultCode(e) ?: return false
    return code in codes
}

/**
 * Checks if a SQLite result code matches a base code (e.g. BUSY/LOCKED), including extended codes.
 * SQLite extended result codes preserve the base code in the low byte.
 */
private fun isSQLiteBaseCode(e: Throwable, baseCode: Int): Boolean {
    val code = sqliteResultCode(e) ?: return false
    return code == baseCode || (code and 0xFF) == baseCode
}

private fun sqliteResultCode(e: Throwable): Int? {
    var t: Throwable? = e
    while (t != null) {
        val className = t.javaClass.name
        if (className.contains("SQLiteException") || className.contains("sqlite")) {
            if (t is SQLException) {
                val ec =
                    try {
                        t.errorCode
                    } catch (_: Exception) {
                        null
                    }
                if (ec != null && ec != 0) return ec
            }

            val reflected = trySQLiteResultCodeReflectively(t)
            if (reflected != null && reflected != 0) return reflected

            if (t is SQLException) {
                val ec =
                    try {
                        t.errorCode
                    } catch (_: Exception) {
                        null
                    }
                if (ec != null) return ec
            }
        }

        t = t.cause
        if (t === e) break
    }

    return null
}

private fun trySQLiteResultCodeReflectively(e: Throwable): Int? {
    return try {
        val getResultCode =
            e.javaClass.methods.firstOrNull { it.name == "getResultCode" && it.parameterCount == 0 }
        if (getResultCode != null) {
            val resultCode = getResultCode.invoke(e) ?: return null
            val codeField =
                resultCode.javaClass.declaredFields.firstOrNull {
                    it.name.equals("code", ignoreCase = true)
                }
            if (codeField != null) {
                codeField.isAccessible = true
                return when (val v = codeField.get(resultCode)) {
                    is Int -> v
                    is Number -> v.toInt()
                    else -> null
                }
            }
        }
        null
    } catch (_: Exception) {
        null
    }
}

private fun isSQLiteException(e: Throwable): Boolean {
    var t: Throwable? = e
    while (t != null) {
        val className = t.javaClass.name
        if (className.contains("SQLiteException") || className.contains("sqlite")) {
            return true
        }
        t = t.cause
        if (t === e) break
    }
    return false
}
