package org.funfix.delayedqueue.jvm

import java.time.Instant

/**
 * Message envelope that includes an acknowledgment callback.
 *
 * This wrapper is returned when polling messages from the queue. It contains the message payload
 * plus metadata and an acknowledgment function that should be called after processing completes.
 *
 * This type is not serializable.
 *
 * ## Java Usage
 *
 * ```java
 * AckEnvelope<String> envelope = queue.poll();
 * try {
 *     processMessage(envelope.payload());
 *     envelope.acknowledge();
 * } catch (Exception e) {
 *     // Don't acknowledge - message will be redelivered
 * }
 * ```
 *
 * @param A the type of the message payload
 * @property payload the actual message content
 * @property messageId unique identifier for tracking this message
 * @property timestamp when this envelope was created (poll time)
 * @property source identifier for the queue or source system
 * @property deliveryType indicates whether this is the first delivery or a redelivery
 * @property acknowledge function to call to acknowledge successful processing, and delete the
 *   message from the queue
 */
@JvmRecord
public data class AckEnvelope<out A>(
    val payload: A,
    val messageId: MessageId,
    val timestamp: Instant,
    val source: String,
    val deliveryType: DeliveryType,
    private val acknowledge: AcknowledgeFun,
) {
    /**
     * Acknowledges successful processing of this message.
     *
     * Call this method after the message has been successfully processed. If you don't call
     * acknowledge, the message will be redelivered after the acquire timeout expires.
     *
     * Note: If the message was updated between polling and acknowledgment, the acknowledgment will
     * be ignored to preserve the updated message.
     */
    public fun acknowledge() {
        acknowledge.invoke()
    }
}

/** Handles acknowledgment for a polled message. */
public fun interface AcknowledgeFun {
    /** Acknowledge successful processing. */
    public operator fun invoke()
}

/**
 * Unique identifier for a message.
 *
 * @property value the string representation of the message ID
 */
@JvmRecord
public data class MessageId(public val value: String) {
    override fun toString(): String = value
}

/** Indicates whether a message is being delivered for the first time or redelivered. */
public enum class DeliveryType {
    /** Message is being delivered for the first time. */
    FIRST_DELIVERY,

    /** Message is being redelivered (was scheduled again after initial delivery). */
    REDELIVERY,
}
