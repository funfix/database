package org.funfix.delayedqueue.jvm.internals.utils

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import org.funfix.delayedqueue.jvm.RetryConfig

internal fun RetryConfig.start(now: Instant): Evolution =
    Evolution(
        config = this,
        startedAt = now,
        timeoutAt = totalSoftTimeout?.let { now.plus(it) },
        retriesRemaining = maxRetries,
        delay = initialDelay,
        evolutions = 0L,
        thrownExceptions = emptyList(),
    )

internal data class Evolution(
    val config: RetryConfig,
    val startedAt: Instant,
    val timeoutAt: Instant?,
    val retriesRemaining: Long?,
    val delay: Duration,
    val evolutions: Long,
    val thrownExceptions: List<Throwable>,
) {
    fun canRetry(now: Instant): Boolean {
        val hasRetries = retriesRemaining?.let { it > 0 } ?: true
        val isActive = timeoutAt?.let { now.plus(delay).isBefore(it) } ?: true
        return hasRetries && isActive
    }

    fun timeElapsed(now: Instant): Duration = Duration.between(startedAt, now)

    fun evolve(ex: Throwable?): Evolution =
        copy(
            evolutions = evolutions + 1,
            retriesRemaining = retriesRemaining?.let { maxOf(it - 1, 0) },
            delay = min(delay.multipliedBy(config.backoffFactor.toLong()), config.maxDelay),
            thrownExceptions = ex?.let { listOf(it) + thrownExceptions } ?: thrownExceptions,
        )

    fun prepareException(lastException: Throwable): Throwable {
        val seen = mutableSetOf<ExceptionIdentity>()
        seen.add(ExceptionIdentity(lastException))

        for (suppressed in thrownExceptions) {
            val identity = ExceptionIdentity(suppressed)
            if (!seen.contains(identity)) {
                seen.add(identity)
                lastException.addSuppressed(suppressed)
            }
        }
        return lastException
    }
}

private data class ExceptionIdentity(
    val type: Class<*>,
    val message: String?,
    val causeIdentity: ExceptionIdentity?,
) {
    companion object {
        operator fun invoke(e: Throwable): ExceptionIdentity =
            ExceptionIdentity(
                type = e.javaClass,
                message = e.message,
                causeIdentity = e.cause?.let { invoke(it) },
            )
    }
}

private fun min(a: Duration, b: Duration): Duration = if (a.compareTo(b) <= 0) a else b

internal enum class RetryOutcome {
    RETRY,
    RAISE,
}

internal class ResourceUnavailableException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

internal class RequestTimeoutException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

context(_: Raise<InterruptedException>, _: Raise<TimeoutException>)
internal fun <T> withRetries(
    config: RetryConfig,
    shouldRetry: (Throwable) -> RetryOutcome,
    block: () -> T,
): T {
    var state = config.start(Instant.now())

    while (true) {
        try {
            return if (config.perTryHardTimeout != null) {
                withTimeout(config.perTryHardTimeout) { block() }
            } else {
                block()
            }
        } catch (e: Throwable) {
            val now = Instant.now()

            if (!state.canRetry(now)) {
                throw createFinalException(state, e, now)
            }

            val outcome =
                try {
                    shouldRetry(e)
                } catch (predicateError: Throwable) {
                    RetryOutcome.RAISE
                }

            when (outcome) {
                RetryOutcome.RAISE -> throw createFinalException(state, e, now)
                RetryOutcome.RETRY -> {
                    Thread.sleep(state.delay.toMillis())
                    state = state.evolve(e)
                }
            }
        }
    }
}

private fun createFinalException(state: Evolution, e: Throwable, now: Instant): Throwable {
    val elapsed = state.timeElapsed(now)
    return when {
        e is TimeoutExceptionWrapper -> {
            state.prepareException(
                RequestTimeoutException(
                    "Giving up after ${state.evolutions} retries and $elapsed",
                    e.cause,
                )
            )
        }
        else -> {
            ResourceUnavailableException(
                "Giving up after ${state.evolutions} retries and $elapsed",
                state.prepareException(e),
            )
        }
    }
}

private class TimeoutExceptionWrapper(cause: Throwable) : RuntimeException(cause)

context(_: Raise<InterruptedException>)
private fun <T> withTimeout(timeout: Duration, block: () -> T): T {
    val task = org.funfix.tasks.jvm.Task.fromBlockingIO { block() }
    val fiber = task.ensureRunningOnExecutor(DB_EXECUTOR).runFiber()

    try {
        return fiber.awaitBlockingTimed(timeout)
    } catch (e: TimeoutException) {
        fiber.cancel()
        fiber.joinBlockingUninterruptible()
        // Wrap in our internal wrapper to distinguish from user TimeoutExceptions
        throw TimeoutExceptionWrapper(e)
    } catch (e: ExecutionException) {
        val cause = e.cause
        when {
            cause != null -> throw cause
            else -> throw e
        }
    } catch (e: InterruptedException) {
        fiber.cancel()
        fiber.joinBlockingUninterruptible()
        raise(e)
    }
}
