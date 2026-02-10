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

package org.funfix.delayedqueue.scala

import cats.effect.IO
import java.time.Instant

/** A delayed queue for scheduled message processing with FIFO semantics.
  *
  * @tparam A
  *   the type of message payloads stored in the queue
  */
trait DelayedQueue[A] {

  /** Returns the [DelayedQueueTimeConfig] with which this instance was
    * initialized.
    */
  def getTimeConfig: IO[DelayedQueueTimeConfig]

  /** Offers a message for processing, at a specific timestamp.
    *
    * In case the key already exists, an update is attempted.
    *
    * @param key
    *   identifies the message; can be a "transaction ID" that could later be
    *   used for deleting the message in advance
    * @param payload
    *   is the message being delivered
    * @param scheduleAt
    *   specifies when the message will become available for `poll` and
    *   processing
    */
  def offerOrUpdate(key: String, payload: A, scheduleAt: Instant): IO[OfferOutcome]

  /** Version of [offerOrUpdate] that only creates new entries and does not
    * allow updates.
    */
  def offerIfNotExists(key: String, payload: A, scheduleAt: Instant): IO[OfferOutcome]

  /** Batched version of offer operations.
    *
    * @param In
    *   is the type of the input message, corresponding to each
    *   [ScheduledMessage]. This helps in streaming the original input messages
    *   after processing the batch.
    */
  def offerBatch[In](messages: List[BatchedMessage[In, A]]): IO[List[BatchedReply[In, A]]]

  /** Pulls the first message to process from the queue (FIFO), returning `None`
    * in case no such message is available.
    *
    * This method locks the message for processing, making it invisible for
    * other consumers (until the configured timeout happens).
    */
  def tryPoll: IO[Option[AckEnvelope[A]]]

  /** Pulls a batch of messages to process from the queue (FIFO), returning an
    * empty list in case no such messages are available.
    *
    * WARNING: don't abuse the number of messages requested. E.g., a large
    * number, such as 20000, can still lead to serious performance issues.
    *
    * @param batchMaxSize
    *   is the maximum number of messages that can be returned in a single
    *   batch; the actual number of returned messages can be smaller than this
    *   value, depending on how many messages are available at the time of
    *   polling
    */
  def tryPollMany(batchMaxSize: Int): IO[AckEnvelope[List[A]]]

  /** Extracts the next event from the delayed-queue, or waits until there's
    * such an event available.
    */
  def poll: IO[AckEnvelope[A]]

  /** Reads a message from the queue, corresponding to the given `key`, without
    * locking it for processing.
    *
    * This is unlike [tryPoll] or [poll], because multiple consumers can read
    * the same message. Use with care, because processing a message retrieved
    * via [read] does not guarantee that the message will be processed only
    * once.
    *
    * WARNING: this operation invalidates the model of the queue. DO NOT USE!
    * This is because multiple consumers can process the same message, leading
    * to potential issues.
    */
  def read(key: String): IO[Option[AckEnvelope[A]]]

  /** Deletes a message from the queue that's associated with the given `key`.
    */
  def dropMessage(key: String): IO[Boolean]

  /** Checks that a message exists in the queue.
    *
    * @param key
    *   identifies the message
    * @return
    *   `true` in case a message with the given `key` exists in the queue,
    *   `false` otherwise
    */
  def containsMessage(key: String): IO[Boolean]

  /** Drops all existing enqueued messages.
    *
    * This deletes all messages from the DB table of the configured type.
    *
    * WARN: This is a dangerous operation, because it can lead to data loss. Use
    * with care, i.e., only for testing!
    *
    * @param confirm
    *   must be exactly "Yes, please, I know what I'm doing!" to proceed
    * @return
    *   the number of messages deleted
    */
  def dropAllMessages(confirm: String): IO[Int]

  /** Utilities for installing cron-like schedules. */
  def cron: IO[CronService[A]]
}
