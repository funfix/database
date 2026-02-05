package org.funfix.delayedqueue.jvm

import java.time.Instant

/**
 * Represents a message scheduled for future delivery in the delayed queue.
 *
 * This is the primary data structure for messages that will be processed at a specific time in the
 * future.
 *
 * @param A the type of the message payload
 * @property key unique identifier for this message; can be used to update or delete the message
 * @property payload the actual message content
 * @property scheduleAt the timestamp when this message becomes available for polling
 * @property canUpdate whether existing messages with the same key can be updated
 */
@JvmRecord
public data class ScheduledMessage<out A>
@JvmOverloads
constructor(val key: String, val payload: A, val scheduleAt: Instant, val canUpdate: Boolean = true)

/**
 * Wrapper for batched message operations, associating input metadata with scheduled messages.
 *
 * @param In the type of the input metadata
 * @param A the type of the message payload
 * @property input the original input metadata
 * @property message the scheduled message
 */
@JvmRecord
public data class BatchedMessage<In, A>(val input: In, val message: ScheduledMessage<A>) {
    /** Creates a reply for this batched message with the given outcome. */
    public fun reply(outcome: OfferOutcome): BatchedReply<In, A> =
        BatchedReply(input, message, outcome)
}

/**
 * Reply for a batched message operation, containing the outcome.
 *
 * @param In the type of the input metadata
 * @param A the type of the message payload
 * @property input the original input metadata
 * @property message the scheduled message
 * @property outcome the result of offering this message
 */
@JvmRecord
public data class BatchedReply<In, A>(
    val input: In,
    val message: ScheduledMessage<A>,
    val outcome: OfferOutcome,
)
