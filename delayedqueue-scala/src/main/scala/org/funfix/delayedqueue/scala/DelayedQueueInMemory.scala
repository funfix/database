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

import cats.effect.{IO, Resource, Clock}
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
  def apply[A](
      timeConfig: DelayedQueueTimeConfig = DelayedQueueTimeConfig.DEFAULT_IN_MEMORY,
      ackEnvSource: String = "delayed-queue-inmemory"
  ): Resource[IO, DelayedQueue[A]] =
    Dispatcher.sequential[IO].evalMap { dispatcher =>
      IO {
        val javaClock = CatsClockToJavaClock(dispatcher)
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
  private class DelayedQueueInMemoryWrapper[A](
      underlying: jvm.DelayedQueueInMemory[A]
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
  private class CronServiceWrapper[A](
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

private final class CatsClockToJavaClock(
    dispatcher: Dispatcher[IO],
    zone: java.time.ZoneId = java.time.ZoneId.systemDefault()
)(using Clock[IO]) extends JavaClock {
  override def getZone: java.time.ZoneId =
    zone

  override def withZone(zone: java.time.ZoneId): JavaClock =
    new CatsClockToJavaClock(dispatcher, zone)

  override def instant(): Instant =
    dispatcher.unsafeRunSync(
      Clock[IO].realTime.map(it => Instant.ofEpochMilli(it.toMillis))
    )
}
