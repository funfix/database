package org.funfix.delayedqueue.jvm

import java.time.Duration

/**
 * Time configuration for delayed queue operations.
 *
 * @property acquireTimeout maximum time to wait when acquiring/locking a message for processing
 * @property pollPeriod interval between poll attempts when no messages are available
 */
@JvmRecord
public data class DelayedQueueTimeConfig
@JvmOverloads
constructor(
    val acquireTimeout: Duration = Duration.ofSeconds(30),
    val pollPeriod: Duration = Duration.ofMillis(100),
) {
    public companion object {
        /** Default configuration with 30-second acquire timeout and 100ms poll period. */
        @JvmField public val DEFAULT: DelayedQueueTimeConfig = DelayedQueueTimeConfig()

        /** Creates a time configuration with the specified values. */
        @JvmStatic
        public fun create(acquireTimeout: Duration, pollPeriod: Duration): DelayedQueueTimeConfig =
            DelayedQueueTimeConfig(acquireTimeout, pollPeriod)
    }
}
