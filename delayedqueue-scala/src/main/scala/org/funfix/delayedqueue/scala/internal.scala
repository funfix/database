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

import cats.effect.{IO, Resource}
import cats.syntax.functor.*
import java.time.Instant
import org.funfix.delayedqueue.jvm
import org.funfix.delayedqueue.scala.AckEnvelope.asScala
import org.funfix.delayedqueue.scala.OfferOutcome.asScala
import org.funfix.delayedqueue.scala.BatchedReply.asScala
import org.funfix.delayedqueue.scala.DelayedQueueTimeConfig.asScala
import scala.jdk.CollectionConverters.*

/** Internal wrappers shared between DelayedQueueInMemory and DelayedQueueJDBC implementations. */
private[scala] object internal {

  /** Wrapper that implements the Scala DelayedQueue trait by delegating to a JVM DelayedQueue
    * implementation.
    */
  private[scala] class DelayedQueueWrapper[A](
      underlying: jvm.DelayedQueue[A]
  ) extends DelayedQueue[A] {

    override def getTimeConfig: IO[DelayedQueueTimeConfig] =
      IO(underlying.getTimeConfig.asScala)

    override def offerOrUpdate(key: String, payload: A, scheduleAt: Instant): IO[OfferOutcome] =
      IO(underlying.offerOrUpdate(key, payload, scheduleAt).asScala)

    override def offerIfNotExists(key: String, payload: A, scheduleAt: Instant): IO[OfferOutcome] =
      IO(underlying.offerIfNotExists(key, payload, scheduleAt).asScala)

    override def offerBatch[In](
        messages: List[BatchedMessage[In, A]]
    ): IO[List[BatchedReply[In, A]]] =
      IO {
        val javaMessages = messages.map(_.asJava).asJava
        val javaReplies = underlying.offerBatch(javaMessages)
        javaReplies.asScala.toList.map(_.asScala)
      }

    override def tryPoll: IO[Option[AckEnvelope[A]]] =
      IO {
        Option(underlying.tryPoll()).map(_.asScala)
      }

    override def tryPollMany(batchMaxSize: Int): IO[AckEnvelope[List[A]]] =
      IO {
        val javaEnvelope = underlying.tryPollMany(batchMaxSize)
        AckEnvelope(
          payload = javaEnvelope.payload.asScala.toList,
          messageId = MessageId.asScala(javaEnvelope.messageId),
          timestamp = javaEnvelope.timestamp,
          source = javaEnvelope.source,
          deliveryType = DeliveryType.asScala(javaEnvelope.deliveryType),
          acknowledge = IO.blocking(javaEnvelope.acknowledge())
        )
      }

    override def poll: IO[AckEnvelope[A]] =
      IO.interruptible(underlying.poll().asScala)

    override def read(key: String): IO[Option[AckEnvelope[A]]] =
      IO {
        Option(underlying.read(key)).map(_.asScala)
      }

    override def dropMessage(key: String): IO[Boolean] =
      IO(underlying.dropMessage(key))

    override def containsMessage(key: String): IO[Boolean] =
      IO(underlying.containsMessage(key))

    override def dropAllMessages(confirm: String): IO[Int] =
      IO(underlying.dropAllMessages(confirm))

    override def cron: IO[CronService[A]] =
      IO(new CronServiceWrapper(underlying.getCron))
  }

  /** Wrapper for CronService that delegates to the JVM implementation. */
  private[scala] class CronServiceWrapper[A](
      underlying: jvm.CronService[A]
  ) extends CronService[A] {

    override def installTick(
        configHash: CronConfigHash,
        keyPrefix: String,
        messages: List[CronMessage[A]]
    ): IO[Unit] =
      IO {
        val javaMessages = messages.map(_.asJava).asJava
        underlying.installTick(configHash.asJava, keyPrefix, javaMessages)
      }

    override def uninstallTick(configHash: CronConfigHash, keyPrefix: String): IO[Unit] =
      IO {
        underlying.uninstallTick(configHash.asJava, keyPrefix)
      }

    override def install(
        configHash: CronConfigHash,
        keyPrefix: String,
        scheduleInterval: java.time.Duration,
        generateMany: (Instant) => List[CronMessage[A]]
    ): Resource[IO, Unit] =
      Resource.fromAutoCloseable(IO {
        underlying.install(
          configHash.asJava,
          keyPrefix,
          scheduleInterval,
          now => generateMany(now).map(_.asJava).asJava
        )
      }).void

    override def installDailySchedule(
        keyPrefix: String,
        schedule: CronDailySchedule,
        generator: (Instant) => CronMessage[A]
    ): Resource[IO, Unit] =
      Resource.fromAutoCloseable(IO {
        underlying.installDailySchedule(
          keyPrefix,
          schedule.asJava,
          now => generator(now).asJava
        )
      }).void

    override def installPeriodicTick(
        keyPrefix: String,
        period: java.time.Duration,
        generator: (Instant) => A
    ): Resource[IO, Unit] =
      Resource.fromAutoCloseable(IO {
        underlying.installPeriodicTick(
          keyPrefix,
          period,
          now => generator(now)
        )
      }).void
  }
}
