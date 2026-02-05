package org.funfix.delayedqueue.jvm

import java.time.Duration

/**
 * Configuration for retry loops with exponential backoff.
 *
 * Used to configure retry behavior for database operations that may experience transient failures
 * such as deadlocks, connection issues, or transaction rollbacks.
 *
 * ## Example
 *
 * ```kotlin
 * val config = RetryConfig(
 *     maxRetries = 3,
 *     totalSoftTimeout = Duration.ofSeconds(30),
 *     perTryHardTimeout = Duration.ofSeconds(10),
 *     initialDelay = Duration.ofMillis(100),
 *     maxDelay = Duration.ofSeconds(5),
 *     backoffFactor = 2.0
 * )
 * ```
 *
 * ## Java Usage
 *
 * ```java
 * RetryConfig config = new RetryConfig(
 *     3L,                          // maxRetries
 *     Duration.ofSeconds(30),      // totalSoftTimeout
 *     Duration.ofSeconds(10),      // perTryHardTimeout
 *     Duration.ofMillis(100),      // initialDelay
 *     Duration.ofSeconds(5),       // maxDelay
 *     2.0                          // backoffFactor
 * );
 * ```
 *
 * @param maxRetries Maximum number of retries (null means unlimited retries)
 * @param totalSoftTimeout Total time after which retries stop (null means no timeout)
 * @param perTryHardTimeout Hard timeout for each individual attempt (null means no per-try timeout)
 * @param initialDelay Initial delay before first retry
 * @param maxDelay Maximum delay between retries (backoff is capped at this value)
 * @param backoffFactor Multiplier for exponential backoff (e.g., 2.0 for doubling delays)
 */
@JvmRecord
public data class RetryConfig(
    val maxRetries: Long?,
    val totalSoftTimeout: Duration?,
    val perTryHardTimeout: Duration?,
    val initialDelay: Duration,
    val maxDelay: Duration,
    val backoffFactor: Double,
) {
    init {
        require(backoffFactor >= 1.0) { "backoffFactor must be >= 1.0, got $backoffFactor" }
        require(!initialDelay.isNegative) { "initialDelay must not be negative, got $initialDelay" }
        require(!maxDelay.isNegative) { "maxDelay must not be negative, got $maxDelay" }
        require(maxRetries == null || maxRetries >= 0) {
            "maxRetries must be >= 0 or null, got $maxRetries"
        }
        require(totalSoftTimeout == null || !totalSoftTimeout.isNegative) {
            "totalSoftTimeout must not be negative, got $totalSoftTimeout"
        }
        require(perTryHardTimeout == null || !perTryHardTimeout.isNegative) {
            "perTryHardTimeout must not be negative, got $perTryHardTimeout"
        }
    }

    public companion object {
        /**
         * Default retry configuration with reasonable defaults for database operations:
         * - 5 retries maximum
         * - 30 second total timeout
         * - 10 second per-try timeout
         * - 100ms initial delay
         * - 5 second max delay
         * - 2.0 backoff factor (exponential doubling)
         */
        @JvmField
        public val DEFAULT: RetryConfig =
            RetryConfig(
                maxRetries = 5,
                totalSoftTimeout = Duration.ofSeconds(30),
                perTryHardTimeout = Duration.ofSeconds(10),
                initialDelay = Duration.ofMillis(100),
                maxDelay = Duration.ofSeconds(5),
                backoffFactor = 2.0,
            )

        /** No retries - operations fail immediately on first error. */
        @JvmField
        public val NO_RETRIES: RetryConfig =
            RetryConfig(
                maxRetries = 0,
                totalSoftTimeout = null,
                perTryHardTimeout = null,
                initialDelay = Duration.ZERO,
                maxDelay = Duration.ZERO,
                backoffFactor = 1.0,
            )
    }
}
