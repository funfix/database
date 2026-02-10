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

import java.time.Instant
import org.funfix.delayedqueue.jvm

/** Represents a message scheduled for future delivery in the delayed queue.
  *
  * This is the primary data structure for messages that will be processed at a
  * specific time in the future.
  *
  * @tparam A
  *   the type of the message payload
  * @param key
  *   unique identifier for this message; can be used to update or delete the
  *   message
  * @param payload
  *   the actual message content
  * @param scheduleAt
  *   the timestamp when this message becomes available for polling
  * @param canUpdate
  *   whether existing messages with the same key can be updated
  */
final case class ScheduledMessage[+A](
    key: String,
    payload: A,
    scheduleAt: Instant,
    canUpdate: Boolean = true
) {

  /** Converts this Scala ScheduledMessage to a JVM ScheduledMessage. */
  def asJava[A1 >: A]: jvm.ScheduledMessage[A1] =
    new jvm.ScheduledMessage[A1](key, payload, scheduleAt, canUpdate)
}

object ScheduledMessage {

  /** Conversion extension for JVM ScheduledMessage. */
  extension [A](javaMsg: jvm.ScheduledMessage[A]) {

    /** Converts a JVM ScheduledMessage to a Scala ScheduledMessage. */
    def asScala: ScheduledMessage[A] =
      ScheduledMessage(
        key = javaMsg.key,
        payload = javaMsg.payload,
        scheduleAt = javaMsg.scheduleAt,
        canUpdate = javaMsg.canUpdate
      )
  }
}

/** Wrapper for batched message operations, associating input metadata with
  * scheduled messages.
  *
  * @tparam In
  *   the type of the input metadata
  * @tparam A
  *   the type of the message payload
  * @param input
  *   the original input metadata
  * @param message
  *   the scheduled message
  */
final case class BatchedMessage[+In, +A](
    input: In,
    message: ScheduledMessage[A]
) {

  /** Creates a reply for this batched message with the given outcome. */
  def reply(outcome: OfferOutcome): BatchedReply[In, A] =
    BatchedReply(input, message, outcome)

  /** Converts this Scala BatchedMessage to a JVM BatchedMessage. */
  def asJava[In1 >: In, A1 >: A]: jvm.BatchedMessage[In1, A1] =
    new jvm.BatchedMessage[In1, A1](input, message.asJava)
}

object BatchedMessage {

  /** Conversion extension for JVM BatchedMessage. */
  extension [In, A](javaMsg: jvm.BatchedMessage[In, A]) {

    /** Converts a JVM BatchedMessage to a Scala BatchedMessage. */
    def asScala: BatchedMessage[In, A] =
      BatchedMessage(
        input = javaMsg.input,
        message = ScheduledMessage.asScala(javaMsg.message)
      )
  }
}

/** Reply for a batched message operation, containing the outcome.
  *
  * @tparam In
  *   the type of the input metadata
  * @tparam A
  *   the type of the message payload
  * @param input
  *   the original input metadata
  * @param message
  *   the scheduled message
  * @param outcome
  *   the result of offering this message
  */
final case class BatchedReply[+In, +A](
    input: In,
    message: ScheduledMessage[A],
    outcome: OfferOutcome
) {

  /** Converts this Scala BatchedReply to a JVM BatchedReply. */
  def asJava[In1 >: In, A1 >: A]: jvm.BatchedReply[In1, A1] =
    new jvm.BatchedReply[In1, A1](input, message.asJava, outcome.asJava)
}

object BatchedReply {

  /** Conversion extension for JVM BatchedReply. */
  extension [In, A](javaReply: jvm.BatchedReply[In, A]) {

    /** Converts a JVM BatchedReply to a Scala BatchedReply. */
    def asScala: BatchedReply[In, A] =
      BatchedReply(
        input = javaReply.input,
        message = ScheduledMessage.asScala(javaReply.message),
        outcome = OfferOutcome.asScala(javaReply.outcome)
      )
  }
}
