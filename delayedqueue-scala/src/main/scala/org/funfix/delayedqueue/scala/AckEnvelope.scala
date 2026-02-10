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
import org.funfix.delayedqueue.jvm

/** Message envelope that includes an acknowledgment callback.
  *
  * This wrapper is returned when polling messages from the queue. It contains
  * the message payload plus metadata and an acknowledgment function that should
  * be called after processing completes.
  *
  * This type is not serializable.
  *
  * ==Example==
  *
  * {{{
  * for {
  *   envelope <- queue.poll
  *   _ <- processMessage(envelope.payload)
  *   _ <- envelope.acknowledge
  * } yield ()
  * }}}
  *
  * @tparam A
  *   the type of the message payload
  * @param payload
  *   the actual message content
  * @param messageId
  *   unique identifier for tracking this message
  * @param timestamp
  *   when this envelope was created (poll time)
  * @param source
  *   identifier for the queue or source system
  * @param deliveryType
  *   indicates whether this is the first delivery or a redelivery
  * @param acknowledge
  *   IO action to call to acknowledge successful processing, and delete the
  *   message from the queue
  */
final case class AckEnvelope[+A](
  payload: A,
  messageId: MessageId,
  timestamp: Instant,
  source: String,
  deliveryType: DeliveryType,
  acknowledge: IO[Unit]
)

object AckEnvelope {

  /** Conversion extension for JVM AckEnvelope. */
  extension [A](javaEnv: jvm.AckEnvelope[A]) {

    /** Converts a JVM AckEnvelope to a Scala AckEnvelope. */
    def asScala: AckEnvelope[A] =
      AckEnvelope(
        payload = javaEnv.payload,
        messageId = MessageId.asScala(javaEnv.messageId),
        timestamp = javaEnv.timestamp,
        source = javaEnv.source,
        deliveryType = DeliveryType.asScala(javaEnv.deliveryType),
        acknowledge = IO.blocking(javaEnv.acknowledge())
      )
  }
}

/** Unique identifier for a message. */
opaque type MessageId = String

object MessageId {

  /** Creates a MessageId from a String value. */
  def apply(value: String): MessageId =
    value

  /** Conversion extension for String to MessageId. */
  extension (id: MessageId) {

    /** Gets the string value of the MessageId. */
    def value: String =
      id

    /** Converts this Scala MessageId to a JVM MessageId. */
    def asJava: jvm.MessageId =
      new jvm.MessageId(id)
  }

  /** Conversion extension for JVM MessageId. */
  extension (javaId: jvm.MessageId) {

    /** Converts a JVM MessageId to a Scala MessageId. */
    def asScala: MessageId =
      MessageId(javaId.value)
  }
}

/** Indicates whether a message is being delivered for the first time or
  * redelivered.
  */
enum DeliveryType {

  /** Message is being delivered for the first time. */
  case FirstDelivery

  /** Message is being redelivered (was scheduled again after initial delivery).
    */
  case Redelivery

  /** Converts this Scala DeliveryType to a JVM DeliveryType. */
  def asJava: jvm.DeliveryType =
    this match {
      case FirstDelivery => jvm.DeliveryType.FIRST_DELIVERY
      case Redelivery    => jvm.DeliveryType.REDELIVERY
    }
}

object DeliveryType {

  /** Conversion extension for JVM DeliveryType. */
  extension (javaType: jvm.DeliveryType) {

    /** Converts a JVM DeliveryType to a Scala DeliveryType. */
    def asScala: DeliveryType =
      javaType match {
        case jvm.DeliveryType.FIRST_DELIVERY => DeliveryType.FirstDelivery
        case jvm.DeliveryType.REDELIVERY     => DeliveryType.Redelivery
      }
  }
}
