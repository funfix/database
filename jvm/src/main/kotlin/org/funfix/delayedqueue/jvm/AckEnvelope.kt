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
 *     processMessage(envelope.getPayload());
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
 */
@NonSerializable
public data class AckEnvelope<out A>(
    val payload: A,
    val messageId: MessageId,
    val timestamp: Instant,
    val source: String,
    val deliveryType: DeliveryType,
    @field:Transient private val acknowledgeHandler: AckHandler = AckHandler {},
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
        acknowledgeHandler.acknowledge()
    }

    /** Companion object for creating AckEnvelopes. */
    public companion object {
        /** Creates an AckEnvelope with the specified properties. */
        @JvmStatic
        @JvmOverloads
        public fun <A> create(
            payload: A,
            messageId: MessageId,
            timestamp: Instant,
            source: String = "delayed-queue",
            deliveryType: DeliveryType = DeliveryType.FIRST_DELIVERY,
            acknowledgeHandler: AckHandler = AckHandler {},
        ): AckEnvelope<A> =
            AckEnvelope(payload, messageId, timestamp, source, deliveryType, acknowledgeHandler)
    }

    // Custom equals/hashCode to exclude acknowledgeHandler
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AckEnvelope<*>) return false
        if (payload != other.payload) return false
        if (messageId != other.messageId) return false
        if (timestamp != other.timestamp) return false
        if (source != other.source) return false
        if (deliveryType != other.deliveryType) return false
        return true
    }

    override fun hashCode(): Int {
        var result = payload?.hashCode() ?: 0
        result = 31 * result + messageId.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + source.hashCode()
        result = 31 * result + deliveryType.hashCode()
        return result
    }

    override fun toString(): String {
        return "AckEnvelope(payload=$payload, messageId=$messageId, timestamp=$timestamp, source='$source', deliveryType=$deliveryType)"
    }
}

/**
 * Unique identifier for a message.
 *
 * @property value the string representation of the message ID
 */
@JvmInline
public value class MessageId(public val value: String) {
    override fun toString(): String = value
}

/** Indicates whether a message is being delivered for the first time or redelivered. */
public enum class DeliveryType {
    /** Message is being delivered for the first time. */
    FIRST_DELIVERY,

    /** Message is being redelivered (was scheduled again after initial delivery). */
    REDELIVERY,
}
