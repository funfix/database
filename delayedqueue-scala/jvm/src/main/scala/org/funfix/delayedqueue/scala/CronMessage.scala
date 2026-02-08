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
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.funfix.delayedqueue.jvm

/** Represents a message for periodic (cron-like) scheduling.
  *
  * This wrapper is used for messages that should be scheduled repeatedly. The `scheduleAt` is used
  * to generate the unique key, while `scheduleAtActual` allows for a different execution time
  * (e.g., to add a delay).
  *
  * @tparam A
  *   the type of the message payload
  * @param payload
  *   the message content
  * @param scheduleAt
  *   the nominal schedule time (used for key generation)
  * @param scheduleAtActual
  *   the actual execution time (defaults to scheduleAt if None)
  */
final case class CronMessage[+A](
    payload: A,
    scheduleAt: Instant,
    scheduleAtActual: Option[Instant] = None
) {

  /** Converts this CronMessage to a ScheduledMessage.
    *
    * @param configHash
    *   the configuration hash for this cron job
    * @param keyPrefix
    *   the prefix for generating unique keys
    * @param canUpdate
    *   whether the resulting message can update existing entries
    */
  def toScheduled(
      configHash: CronConfigHash,
      keyPrefix: String,
      canUpdate: Boolean
  ): ScheduledMessage[A] =
    ScheduledMessage(
      key = CronMessage.key(configHash, keyPrefix, scheduleAt),
      payload = payload,
      scheduleAt = scheduleAtActual.getOrElse(scheduleAt),
      canUpdate = canUpdate
    )

  /** Converts this Scala CronMessage to a JVM CronMessage. */
  def asJava[A1 >: A]: jvm.CronMessage[A1] =
    new jvm.CronMessage[A1](payload, scheduleAt, scheduleAtActual.getOrElse(null))
}

object CronMessage {
  private val CRON_DATE_TIME_FORMATTER: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC)
  private val NANOS_WIDTH = 9

  private def formatTimestamp(scheduleAt: Instant): String = {
    val nanos = String.format(Locale.ROOT, s"%0${NANOS_WIDTH}d", scheduleAt.getNano: Integer)
    s"${CRON_DATE_TIME_FORMATTER.format(scheduleAt)}.$nanos"
  }

  /** Generates a unique key for a cron message.
    *
    * @param configHash
    *   the configuration hash
    * @param keyPrefix
    *   the key prefix
    * @param scheduleAt
    *   the schedule time
    * @return
    *   a unique key string
    */
  def key(configHash: CronConfigHash, keyPrefix: String, scheduleAt: Instant): String =
    s"$keyPrefix/${configHash.value}/${formatTimestamp(scheduleAt)}"

  /** Creates a factory function that produces CronMessages with a static payload.
    *
    * @param payload
    *   the static payload to use for all generated messages
    * @return
    *   a function that creates CronMessages for any given instant
    */
  def staticPayload[A](payload: A): CronMessageGenerator[A] =
    (scheduleAt: Instant) => CronMessage(payload, scheduleAt)

  /** Conversion extension for JVM CronMessage. */
  extension [A](javaMsg: jvm.CronMessage[A]) {

    /** Converts a JVM CronMessage to a Scala CronMessage. */
    def asScala: CronMessage[A] =
      CronMessage(
        payload = javaMsg.payload,
        scheduleAt = javaMsg.scheduleAt,
        scheduleAtActual = Option(javaMsg.scheduleAtActual)
      )
  }
}

/** Function that generates CronMessages for given instants. */
trait CronMessageGenerator[A] {

  /** Creates a cron message for the given instant. */
  def apply(at: Instant): CronMessage[A]
}
