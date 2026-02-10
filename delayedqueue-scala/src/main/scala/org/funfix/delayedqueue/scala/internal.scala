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
import cats.effect.Resource
import cats.syntax.functor.*
import java.time.Instant
import org.funfix.delayedqueue.jvm
import scala.jdk.CollectionConverters.*

/** Wrapper that implements the Scala DelayedQueue trait by delegating to a JVM
  * DelayedQueue implementation.
  */
private[scala] class DelayedQueueWrapper[A](
  underlying: jvm.DelayedQueue[A]
) extends DelayedQueue[A] {

  override def getTimeConfig: IO[DelayedQueueTimeConfig] =
    IO(DelayedQueueTimeConfig.fromJava(underlying.getTimeConfig))

  override def offerOrUpdate(key: String, payload: A, scheduleAt: Instant): IO[OfferOutcome] =
    IO(OfferOutcome.fromJava(underlying.offerOrUpdate(key, payload, scheduleAt)))

  override def offerIfNotExists(key: String, payload: A, scheduleAt: Instant): IO[OfferOutcome] =
    IO(OfferOutcome.fromJava(underlying.offerIfNotExists(key, payload, scheduleAt)))

  override def offerBatch[In](
    messages: List[BatchedMessage[In, A]]
  ): IO[List[BatchedReply[In, A]]] =
    IO {
      val javaMessages = messages.map(_.asJava).asJava
      val javaReplies = underlying.offerBatch(javaMessages)
      javaReplies.asScala.toList.map(BatchedReply.fromJava)
    }

  override def tryPoll: IO[Option[AckEnvelope[A]]] =
    IO {
      Option(underlying.tryPoll()).map(AckEnvelope.fromJava)
    }

  override def tryPollMany(batchMaxSize: Int): IO[AckEnvelope[List[A]]] =
    IO {
      val javaEnvelope = underlying.tryPollMany(batchMaxSize)
      AckEnvelope(
        payload = javaEnvelope.payload.asScala.toList,
        messageId = MessageId.fromJava(javaEnvelope.messageId),
        timestamp = javaEnvelope.timestamp,
        source = javaEnvelope.source,
        deliveryType = DeliveryType.fromJava(javaEnvelope.deliveryType),
        acknowledge = IO.blocking(javaEnvelope.acknowledge())
      )
    }

  override def poll: IO[AckEnvelope[A]] =
    IO.interruptible(AckEnvelope.fromJava(underlying.poll()))

  override def read(key: String): IO[Option[AckEnvelope[A]]] =
    IO {
      Option(underlying.read(key)).map(AckEnvelope.fromJava)
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
final private[scala] class CronServiceWrapper[A](
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
    generateMany: Instant => List[CronMessage[A]]
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
    generator: Instant => CronMessage[A]
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
    generator: Instant => A
  ): Resource[IO, Unit] =
    Resource.fromAutoCloseable(IO {
      underlying.installPeriodicTick(
        keyPrefix,
        period,
        now => generator(now)
      )
    }).void
}
