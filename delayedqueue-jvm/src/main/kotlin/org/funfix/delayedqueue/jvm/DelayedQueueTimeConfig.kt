package org.funfix.delayedqueue.jvm

import java.time.Duration

/**
 * Time configuration for delayed queue operations.
 *
 * @property acquireTimeout maximum time to wait when acquiring/locking a message for processing
 * @property pollPeriod interval between poll attempts when no messages are available
 */
@JvmRecord
public data class DelayedQueueTimeConfig(val acquireTimeout: Duration, val pollPeriod: Duration) {
    public companion object {
        /** Default configuration for [DelayedQueueInMemory]. */
        @JvmField
        public val DEFAULT_IN_MEMORY: DelayedQueueTimeConfig =
            DelayedQueueTimeConfig(
                acquireTimeout = Duration.ofMinutes(5),
                pollPeriod = Duration.ofMillis(500),
            )

        /**
         * Default configuration for JDBC-based implementations, with longer acquire timeouts and
         * poll periods to reduce database load in production environments.
         */
        @JvmField
        public val DEFAULT_JDBC: DelayedQueueTimeConfig =
            DelayedQueueTimeConfig(
                acquireTimeout = Duration.ofMinutes(5),
                pollPeriod = Duration.ofSeconds(3),
            )

        /**
         * Default configuration for testing, with shorter timeouts and poll periods to speed up
         * tests.
         */
        @JvmField
        public val DEFAULT_TESTING: DelayedQueueTimeConfig =
            DelayedQueueTimeConfig(
                acquireTimeout = Duration.ofSeconds(30),
                pollPeriod = Duration.ofMillis(100),
            )

        /** Creates a time configuration with the specified values. */
        @JvmStatic
        public fun create(acquireTimeout: Duration, pollPeriod: Duration): DelayedQueueTimeConfig =
            DelayedQueueTimeConfig(acquireTimeout, pollPeriod)
    }
}
