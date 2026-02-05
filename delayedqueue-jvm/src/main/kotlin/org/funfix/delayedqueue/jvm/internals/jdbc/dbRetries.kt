package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.RetryConfig
import org.funfix.delayedqueue.jvm.internals.utils.Raise
import org.funfix.delayedqueue.jvm.internals.utils.RetryOutcome
import org.funfix.delayedqueue.jvm.internals.utils.withRetries

/**
 * Executes a database operation with retry logic based on RDBMS-specific exception handling.
 *
 * This function applies retry policies specifically designed for database operations:
 * - Retries on transient failures (deadlocks, connection issues, transaction rollbacks)
 * - Does NOT retry on generic SQLExceptions (likely application errors)
 * - Retries on unexpected non-SQL exceptions (potentially transient infrastructure issues)
 *
 * @param config Retry configuration (backoff, timeouts, max retries)
 * @param filters RDBMS-specific exception filters (default: HSQLDB)
 * @param block The database operation to execute
 * @return The result of the successful operation
 * @throws SQLException if retry policy decides not to retry
 * @throws ResourceUnavailableException if all retries are exhausted
 */
context(_: Raise<InterruptedException>, _: Raise<java.util.concurrent.TimeoutException>)
internal fun <T> withDbRetries(
    config: RetryConfig,
    filters: RdbmsExceptionFilters = HSQLDBFilters,
    block: () -> T,
): T =
    withRetries(
        config,
        shouldRetry = { exception ->
            when {
                filters.transientFailure.matches(exception) -> {
                    // Transient database failures should be retried
                    RetryOutcome.RETRY
                }
                exception is SQLException -> {
                    // Generic SQL exceptions are likely application errors, don't retry
                    RetryOutcome.RAISE
                }
                else -> {
                    // Unexpected exceptions might be transient infrastructure issues
                    RetryOutcome.RETRY
                }
            }
        },
        block,
    )
