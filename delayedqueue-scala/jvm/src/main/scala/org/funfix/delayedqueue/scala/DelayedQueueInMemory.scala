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
import cats.syntax.functor.*
import java.time.{Clock as JavaClock, Instant}
import org.funfix.delayedqueue.jvm
import org.funfix.delayedqueue.scala.AckEnvelope.asScala
import org.funfix.delayedqueue.scala.OfferOutcome.asScala
import org.funfix.delayedqueue.scala.BatchedReply.asScala
import org.funfix.delayedqueue.scala.DelayedQueueTimeConfig.asScala
import scala.jdk.CollectionConverters.*

/** In-memory implementation of [[DelayedQueue]] using concurrent data structures.
  *
  * This implementation wraps the JVM [[org.funfix.delayedqueue.jvm.DelayedQueueInMemory]] and
  * provides an idiomatic Scala API with Cats Effect IO for managing side effects.
  *
  * The underlying implementation uses a ReentrantLock to protect mutable state (compatible with
  * virtual threads) and condition variables for efficient blocking in `poll`.
  *
  * ==Example==
  *
  * {{{
  * import cats.effect.IO
  * import java.time.Instant
  *
  * val queue = DelayedQueueInMemory.create[String]()
  *
  * for {
  *   _ <- queue.offerOrUpdate("key1", "Hello", Instant.now().plusSeconds(10))
  *   envelope <- queue.poll
  *   _ <- IO.println(s"Received: \${envelope.payload}")
  *   _ <- envelope.acknowledge
  * } yield ()
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
  def create[A](
      timeConfig: DelayedQueueTimeConfig = DelayedQueueTimeConfig.DEFAULT_IN_MEMORY,
      ackEnvSource: String = "delayed-queue-inmemory",
      clock: JavaClock = JavaClock.systemUTC()
  ): DelayedQueue[A] = {
    val jvmQueue = jvm.DelayedQueueInMemory.create[A](
      timeConfig.asJava,
      ackEnvSource,
      clock
    )
    new DelayedQueueInMemoryWrapper(jvmQueue)
  }

  /** Wrapper that implements the Scala DelayedQueue trait by delegating to the JVM implementation.
    */
  private class DelayedQueueInMemoryWrapper[A](underlying: jvm.DelayedQueueInMemory[A])
    extends DelayedQueue[A] {

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
      IO.interruptible {
        underlying.poll().asScala
      }

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

    override def getCron: IO[CronService[A]] =
      IO(new CronServiceWrapper(underlying.getCron))
  }

  /** Wrapper for CronService that delegates to the JVM implementation. */
  private class CronServiceWrapper[A](underlying: jvm.CronService[A]) extends CronService[A] {

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
        generateMany: CronMessageBatchGenerator[A]
    ): cats.effect.Resource[IO, Unit] = {
      import cats.effect.Resource

      val acquire = IO {
        val javaGenerator = new jvm.CronMessageBatchGenerator[A] {
          override def invoke(now: Instant): java.util.List[jvm.CronMessage[A]] =
            generateMany(now).map(_.asJava).asJava
        }
        underlying.install(configHash.asJava, keyPrefix, scheduleInterval, javaGenerator)
      }

      val release = (closeable: AutoCloseable) => IO(closeable.close())

      Resource.make(acquire)(release).void
    }

    override def installDailySchedule(
        keyPrefix: String,
        schedule: CronDailySchedule,
        generator: CronMessageGenerator[A]
    ): cats.effect.Resource[IO, Unit] = {
      import cats.effect.Resource

      val acquire = IO {
        val javaGenerator = new jvm.CronMessageGenerator[A] {
          override def invoke(at: Instant): jvm.CronMessage[A] =
            generator(at).asJava
        }
        underlying.installDailySchedule(keyPrefix, schedule.asJava, javaGenerator)
      }

      val release = (closeable: AutoCloseable) => IO(closeable.close())

      Resource.make(acquire)(release).void
    }

    override def installPeriodicTick(
        keyPrefix: String,
        period: java.time.Duration,
        generator: CronPayloadGenerator[A]
    ): cats.effect.Resource[IO, Unit] = {
      import cats.effect.Resource

      val acquire = IO {
        val javaGenerator = new jvm.CronPayloadGenerator[A] {
          override def invoke(at: Instant): A = generator(at)
        }
        underlying.installPeriodicTick(keyPrefix, period, javaGenerator)
      }

      val release = (closeable: AutoCloseable) => IO(closeable.close())

      Resource.make(acquire)(release).void
    }
  }
}
