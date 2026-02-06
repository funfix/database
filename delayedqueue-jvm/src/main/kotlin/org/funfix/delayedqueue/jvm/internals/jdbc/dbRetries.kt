package org.funfix.delayedqueue.jvm.internals.jdbc

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.ResourceUnavailableException
import org.funfix.delayedqueue.jvm.RetryConfig
import org.funfix.delayedqueue.jvm.internals.utils.Raise
import org.funfix.delayedqueue.jvm.internals.utils.RetryOutcome
import org.funfix.delayedqueue.jvm.internals.utils.raise
import org.funfix.delayedqueue.jvm.internals.utils.withRetries

/**
 * Executes a database operation with retry logic based on RDBMS-specific exception handling.
 *
 * This function applies retry policies specifically designed for database operations:
 * - Retries on transient failures (deadlocks, connection issues, transaction rollbacks)
 * - Does NOT retry on generic SQLExceptions (likely application errors)
 * - Retries on unexpected non-SQL exceptions (potentially transient infrastructure issues)
 * - Wraps TimeoutException into ResourceUnavailableException for public API
 *
 * @param config Retry configuration (backoff, timeouts, max retries)
 * @param clock Clock for time operations (enables testing with mocked time)
 * @param filters RDBMS-specific exception filters (must match the actual JDBC driver)
 * @param block The database operation to execute
 * @return The result of the successful operation
 * @throws ResourceUnavailableException if retries are exhausted or timeout occurs
 * @throws InterruptedException if the operation is interrupted
 */
context(_: Raise<ResourceUnavailableException>, _: Raise<InterruptedException>)
internal fun <T> withDbRetries(
    config: RetryConfig,
    clock: java.time.Clock,
    filters: RdbmsExceptionFilters,
    block: () -> T,
): T =
    try {
        withRetries(
            config,
            clock,
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
    } catch (e: java.util.concurrent.TimeoutException) {
        raise(ResourceUnavailableException("Database operation timed out after retries", e))
    }
