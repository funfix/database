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
 * A delayed queue for scheduled message processing with FIFO semantics.
 *
 * @param A the type of message payloads stored in the queue
 */
public interface DelayedQueue<A> {
    /** Returns the [DelayedQueueTimeConfig] with which this instance was initialized. */
    public fun getTimeConfig(): DelayedQueueTimeConfig

    /**
     * Offers a message for processing, at a specific timestamp.
     *
     * In case the key already exists, an update is attempted.
     *
     * @param key identifies the message; can be a "transaction ID" that could later be used for
     *   deleting the message in advance
     * @param payload is the message being delivered
     * @param scheduleAt specifies when the message will become available for `poll` and processing
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class)
    public fun offerOrUpdate(key: String, payload: A, scheduleAt: Instant): OfferOutcome

    /**
     * Version of [offerOrUpdate] that only creates new entries and does not allow updates.
     *
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class)
    public fun offerIfNotExists(key: String, payload: A, scheduleAt: Instant): OfferOutcome

    /**
     * Batched version of offer operations.
     *
     * @param In is the type of the input message, corresponding to each [ScheduledMessage]. This
     *   helps in streaming the original input messages after processing the batch.
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class)
    public fun <In> offerBatch(messages: List<BatchedMessage<In, A>>): List<BatchedReply<In, A>>

    /**
     * Pulls the first message to process from the queue (FIFO), returning `null` in case no such
     * message is available.
     *
     * This method locks the message for processing, making it invisible for other consumers (until
     * the configured timeout happens).
     *
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class) public fun tryPoll(): AckEnvelope<A>?

    /**
     * Pulls a batch of messages to process from the queue (FIFO), returning an empty list in case
     * no such messages are available.
     *
     * WARNING: don't abuse the number of messages requested. E.g., a large number, such as 20000,
     * can still lead to serious performance issues.
     *
     * @param batchMaxSize is the maximum number of messages that can be returned in a single batch;
     *   the actual number of returned messages can be smaller than this value, depending on how
     *   many messages are available at the time of polling
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class)
    public fun tryPollMany(batchMaxSize: Int): AckEnvelope<List<A>>

    /**
     * Extracts the next event from the delayed-queue, or waits until there's such an event
     * available.
     *
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    @Throws(InterruptedException::class) public fun poll(): AckEnvelope<A>

    /**
     * Reads a message from the queue, corresponding to the given `key`, without locking it for
     * processing.
     *
     * This is unlike [tryPoll] or [poll], because multiple consumers can read the same message. Use
     * with care, because processing a message retrieved via [read] does not guarantee that the
     * message will be processed only once.
     *
     * WARNING: this operation invalidates the model of the queue. DO NOT USE! This is because
     * multiple consumers can process the same message, leading to potential issues.
     *
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class) public fun read(key: String): AckEnvelope<A>?

    /**
     * Deletes a message from the queue that's associated with the given `key`.
     *
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class) public fun dropMessage(key: String): Boolean

    /**
     * Checks that a message exists in the queue.
     *
     * @param key identifies the message
     * @return `true` in case a message with the given `key` exists in the queue, `false` otherwise
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(InterruptedException::class) public fun containsMessage(key: String): Boolean

    /**
     * Drops all existing enqueued messages.
     *
     * This deletes all messages from the DB table of the configured type.
     *
     * WARN: This is a dangerous operation, because it can lead to data loss. Use with care, i.e.,
     * only for testing!
     *
     * @param confirm must be exactly "Yes, please, I know what I'm doing!" to proceed
     * @return the number of messages deleted
     * @throws IllegalArgumentException if the confirmation string is incorrect
     * @throws ResourceUnavailableException if the operation fails after retries
     * @throws InterruptedException if the current thread is interrupted
     */
    @Throws(IllegalArgumentException::class, InterruptedException::class)
    public fun dropAllMessages(confirm: String): Int

    /** Utilities for installing cron-like schedules. */
    public fun getCron(): CronService<A>
}
