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
 * @param acknowledge function to call to acknowledge successful processing, and delete the message
 *   from the queue. Accessible via the `acknowledge()` method.
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
     *
     * @throws ResourceUnavailableException if the database connection is unavailable
     * @throws InterruptedException if the thread is interrupted during acknowledgment
     */
    public fun acknowledge() {
        acknowledge.invoke()
    }
}

/**
 * Handles acknowledgment for a polled message.
 *
 * Implementations may throw exceptions if acknowledgment fails, which the caller should handle
 * appropriately. Failed acknowledgments typically result in message redelivery after the acquire
 * timeout expires.
 */
public fun interface AcknowledgeFun {
    /**
     * Acknowledge successful processing.
     *
     * @throws ResourceUnavailableException if the database connection is unavailable
     * @throws InterruptedException if the thread is interrupted during acknowledgment
     */
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
