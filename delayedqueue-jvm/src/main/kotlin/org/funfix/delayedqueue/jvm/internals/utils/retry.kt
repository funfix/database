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

package org.funfix.delayedqueue.jvm.internals.utils

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import kotlin.math.min
import org.funfix.delayedqueue.jvm.ResourceUnavailableException
import org.funfix.delayedqueue.jvm.RetryConfig

internal fun RetryConfig.start(clock: Clock): Evolution =
    Evolution(
        config = this,
        startedAt = Instant.now(clock),
        timeoutAt = totalSoftTimeout?.let { Instant.now(clock).plus(it) },
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
    val thrownExceptions: List<Exception>,
) {
    fun canRetry(now: Instant): Boolean {
        val hasRetries = retriesRemaining?.let { it > 0 } ?: true
        val isActive = timeoutAt?.let { now.plus(delay).isBefore(it) } ?: true
        return hasRetries && isActive
    }

    fun timeElapsed(now: Instant): Duration = Duration.between(startedAt, now)

    fun evolve(ex: Exception?): Evolution =
        copy(
            evolutions = evolutions + 1,
            retriesRemaining = retriesRemaining?.let { maxOf(it - 1, 0) },
            delay =
                Duration.ofMillis(
                    min(
                        (delay.toMillis() * config.backoffFactor).toLong(),
                        config.maxDelay.toMillis(),
                    )
                ),
            thrownExceptions = ex?.let { listOf(it) + thrownExceptions } ?: thrownExceptions,
        )

    fun prepareException(lastException: Exception): Exception {
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
        operator fun invoke(e: Exception): ExceptionIdentity =
            ExceptionIdentity(
                type = e.javaClass,
                message = e.message,
                causeIdentity = e.cause?.let { if (it is Exception) invoke(it) else throw it },
            )
    }
}

internal enum class RetryOutcome {
    RETRY,
    RAISE,
}

internal fun <T> withRetries(
    config: RetryConfig,
    clock: Clock,
    shouldRetry: (Exception) -> RetryOutcome,
    block: () -> T,
): T {
    var state = config.start(clock)

    while (true) {
        try {
            return if (config.perTryHardTimeout != null) {
                withTimeout(config.perTryHardTimeout) { block() }
            } else {
                block()
            }
        } catch (e: Exception) {
            val now = Instant.now(clock)
            if (!state.canRetry(now)) {
                throw createFinalException(state, e, now)
            }

            val outcome =
                try {
                    shouldRetry(e)
                } catch (predicateError: Exception) {
                    e.addSuppressed(predicateError)
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

private fun createFinalException(state: Evolution, e: Exception, now: Instant): Exception {
    val elapsed = state.timeElapsed(now)
    return when {
        e is TimeoutException -> {
            state.prepareException(
                TimeoutException("Giving up after ${state.evolutions} retries and $elapsed").apply {
                    initCause(e.cause)
                }
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

internal fun <T> withTimeout(timeout: Duration, block: () -> T): T {
    val task = org.funfix.tasks.jvm.Task.fromBlockingIO { block() }
    val fiber = task.ensureRunningOnExecutor(DB_EXECUTOR).runFiber()

    try {
        return fiber.awaitBlockingTimed(timeout)
    } catch (e: TimeoutException) {
        fiber.cancel()
        fiber.joinBlockingUninterruptible()
        throw e
    } catch (e: ExecutionException) {
        val cause = e.cause
        when {
            cause != null -> throw cause
            else -> throw e
        }
    } catch (e: InterruptedException) {
        fiber.cancel()
        fiber.joinBlockingUninterruptible()
        throw e
    }
}
