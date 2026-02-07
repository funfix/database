/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
                    e is SQLException && hasSQLServerError(e, 2627, 2601) -> true
                    e is SQLException &&
                        e.errorCode in setOf(2627, 2601) &&
                        e.sqlState == "23000" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
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
