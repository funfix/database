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

import cats.effect.{Sync, Async, Clock, Resource}
import cats.syntax.functor.*
import java.time.{Clock as JavaClock, Instant}
import org.funfix.delayedqueue.jvm
import org.funfix.delayedqueue.scala.AckEnvelope.asScala
import org.funfix.delayedqueue.scala.OfferOutcome.asScala
import org.funfix.delayedqueue.scala.BatchedReply.asScala
import org.funfix.delayedqueue.scala.DelayedQueueTimeConfig.asScala
import scala.jdk.CollectionConverters.*
import cats.effect.std.Dispatcher

/** In-memory implementation of [[DelayedQueue]] using concurrent data structures.
  *
  * This implementation wraps the JVM [[org.funfix.delayedqueue.jvm.DelayedQueueInMemory]] and
  * provides an idiomatic Scala API with Cats Effect IO for managing side effects.
  *
  * ==Example==
  *
  * {{{
  * import cats.effect.IO
  * import java.time.Instant
  *
  * def worker(queue: DelayedQueue[IO, String]): IO[Unit] = {
  *   val process1 = for {
  *     envelope <- queue.poll
  *     _ <- logger.info("Received: " + envelope.payload)
  *     _ <- envelope.acknowledge
  *   } yield ()
  *
  *   process1.attempt
  *     .onErrorHandleWith { error =>
  *       logger.error("Error processing message, will reprocess after timeout", error)
  *     }
  *     .flatMap { _ =>
  *       worker(queue) // Continue processing the next message
  *     }
  * }
  *
  * DelayedQueueInMemory[IO, String]().use { queue =>
  *   worker(queue).background.use { _ =>
  *     // Push one message after 10 seconds
  *     queue.offerOrUpdate("key1", "Hello", Instant.now().plusSeconds(10))
  *   }
  * }
  * }}}
  *
  * @tparam A
  *   the type of message payloads
  */
object DelayedQueueInMemory {

  /** Creates an in-memory delayed queue with default configuration.
    *
    * @tparam A
    *   the type of message payloads
    * @param timeConfig
    *   time configuration (defaults to [[DelayedQueueTimeConfig.DEFAULT_IN_MEMORY]])
    * @param ackEnvSource
    *   source identifier for envelopes (defaults to "delayed-queue-inmemory")
    * @param clock
    *   clock for time operations (defaults to system UTC clock)
    * @return
    *   a new DelayedQueue instance
    */
  def apply[F[_], A](
      timeConfig: DelayedQueueTimeConfig = DelayedQueueTimeConfig.DEFAULT_IN_MEMORY,
      ackEnvSource: String = "delayed-queue-inmemory"
  )(using Async[F], Clock[F]): Resource[F, DelayedQueue[F, A]] =
    Dispatcher.sequential[F].evalMap { dispatcher =>
      Sync[F].delay {
        val javaClock = CatsClockToJavaClock[F](dispatcher)
        val jvmQueue = jvm.DelayedQueueInMemory.create[A](
          timeConfig.asJava,
          ackEnvSource,
          javaClock
        )
        new DelayedQueueInMemoryWrapper(jvmQueue)
      }
    }

  /** Wrapper that implements the Scala DelayedQueue trait by delegating to the JVM implementation.
    */
  private class DelayedQueueInMemoryWrapper[F[_], A](
      underlying: jvm.DelayedQueueInMemory[A]
  )(using Sync[F]) extends DelayedQueue[F, A] {

    override def getTimeConfig: F[DelayedQueueTimeConfig] =
      Sync[F].delay(underlying.getTimeConfig.asScala)

    override def offerOrUpdate(key: String, payload: A, scheduleAt: Instant): F[OfferOutcome] =
      Sync[F].delay(underlying.offerOrUpdate(key, payload, scheduleAt).asScala)

    override def offerIfNotExists(key: String, payload: A, scheduleAt: Instant): F[OfferOutcome] =
      Sync[F].delay(underlying.offerIfNotExists(key, payload, scheduleAt).asScala)

    override def offerBatch[In](
        messages: List[BatchedMessage[In, A]]
    ): F[List[BatchedReply[In, A]]] =
      Sync[F].delay {
        val javaMessages = messages.map(_.asJava).asJava
        val javaReplies = underlying.offerBatch(javaMessages)
        javaReplies.asScala.toList.map(_.asScala)
      }

    override def tryPoll: F[Option[AckEnvelope[F, A]]] =
      Sync[F].delay {
        Option(underlying.tryPoll()).map(_.asScala)
      }

    override def tryPollMany(batchMaxSize: Int): F[AckEnvelope[F, List[A]]] =
      Sync[F].delay {
        val javaEnvelope = underlying.tryPollMany(batchMaxSize)
        AckEnvelope(
          payload = javaEnvelope.payload.asScala.toList,
          messageId = MessageId.asScala(javaEnvelope.messageId),
          timestamp = javaEnvelope.timestamp,
          source = javaEnvelope.source,
          deliveryType = DeliveryType.asScala(javaEnvelope.deliveryType),
          acknowledge = Sync[F].blocking(javaEnvelope.acknowledge())
        )
      }

    override def poll: F[AckEnvelope[F, A]] =
      Sync[F].interruptible {
        underlying.poll().asScala
      }

    override def read(key: String): F[Option[AckEnvelope[F, A]]] =
      Sync[F].delay {
        Option(underlying.read(key)).map(_.asScala)
      }

    override def dropMessage(key: String): F[Boolean] =
      Sync[F].delay(underlying.dropMessage(key))

    override def containsMessage(key: String): F[Boolean] =
      Sync[F].delay(underlying.containsMessage(key))

    override def dropAllMessages(confirm: String): F[Int] =
      Sync[F].delay(underlying.dropAllMessages(confirm))

    override def cron: F[CronService[F, A]] =
      Sync[F].delay(new CronServiceWrapper(underlying.getCron))
  }

  /** Wrapper for CronService that delegates to the JVM implementation. */
  private class CronServiceWrapper[F[_], A](
      underlying: jvm.CronService[A]
  )(using Sync[F]) extends CronService[F, A] {

    override def installTick(
        configHash: CronConfigHash,
        keyPrefix: String,
        messages: List[CronMessage[A]]
    ): F[Unit] =
      Sync[F].delay {
        val javaMessages = messages.map(_.asJava).asJava
        underlying.installTick(configHash.asJava, keyPrefix, javaMessages)
      }

    override def uninstallTick(configHash: CronConfigHash, keyPrefix: String): F[Unit] =
      Sync[F].delay {
        underlying.uninstallTick(configHash.asJava, keyPrefix)
      }

    override def install(
        configHash: CronConfigHash,
        keyPrefix: String,
        scheduleInterval: java.time.Duration,
        generateMany: (Instant) => List[CronMessage[A]]
    ): Resource[F, Unit] = {
      import cats.effect.Resource

      Resource.fromAutoCloseable(Sync[F].delay {
        underlying.install(
          configHash.asJava,
          keyPrefix,
          scheduleInterval,
          now => generateMany(now).map(_.asJava).asJava
        )
      }).void
    }

    override def installDailySchedule(
        keyPrefix: String,
        schedule: CronDailySchedule,
        generator: (Instant) => CronMessage[A]
    ): Resource[F, Unit] =
      Resource.fromAutoCloseable(Sync[F].delay {
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
    ): cats.effect.Resource[F, Unit] =
      Resource.fromAutoCloseable(Sync[F].delay {
        underlying.installPeriodicTick(
          keyPrefix,
          period,
          now => generator(now)
        )
      }).void
  }
}

private final class CatsClockToJavaClock[+F[_]: Sync: Clock](
    dispatcher: Dispatcher[F],
    zone: java.time.ZoneId = JavaClock.systemUTC().getZone
) extends JavaClock {
  override def getZone: java.time.ZoneId =
    zone

  override def withZone(zone: java.time.ZoneId): JavaClock =
    new CatsClockToJavaClock(dispatcher, zone)

  override def instant(): Instant =
    dispatcher.unsafeRunSync(
      Clock[F]
        .realTime
        .map(it => Instant.ofEpochMilli(it.toMillis).nn)
    )
}
