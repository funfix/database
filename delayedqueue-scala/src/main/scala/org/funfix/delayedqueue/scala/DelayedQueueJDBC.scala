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
import java.time.Instant
import org.funfix.delayedqueue.jvm
import org.funfix.delayedqueue.scala.AckEnvelope.asScala
import org.funfix.delayedqueue.scala.OfferOutcome.asScala
import org.funfix.delayedqueue.scala.BatchedReply.asScala
import scala.jdk.CollectionConverters.*

/** JDBC-based implementation of [[DelayedQueue]] with support for multiple database backends.
  *
  * This implementation wraps the JVM [[org.funfix.delayedqueue.jvm.DelayedQueueJDBC]] and provides
  * an idiomatic Scala API with Cats Effect IO for managing side effects.
  *
  * ==Example==
  *
  * {{{
  * import cats.effect.IO
  * import java.time.Instant
  *
  * val dbConfig = JdbcConnectionConfig(
  *   url = "jdbc:h2:mem:testdb",
  *   driver = JdbcDriver.H2,
  *   username = Some("sa"),
  *   password = Some("")
  * )
  *
  * val config = DelayedQueueJDBCConfig.create(
  *   db = dbConfig,
  *   tableName = "delayed_queue",
  *   queueName = "my-queue"
  * )
  *
  * // Run migrations first (once per database)
  * DelayedQueueJDBC.runMigrations[String](MessageSerializer.forStrings, config).unsafeRunSync()
  *
  * // Create and use the queue
  * DelayedQueueJDBC[String](MessageSerializer.forStrings, config).use { queue =>
  *   for {
  *     _ <- queue.offerOrUpdate("key1", "Hello", Instant.now().plusSeconds(10))
  *     envelope <- queue.poll
  *     _ <- IO.println(s"Received: $${envelope.payload}")
  *     _ <- envelope.acknowledge
  *   } yield ()
  * }
  * }}}
  *
  * @tparam A
  *   the type of message payloads
  */
object DelayedQueueJDBC {

  /** Creates a JDBC-based delayed queue with the given configuration.
    *
    * @tparam A
    *   the type of message payloads
    * @param serializer
    *   serializer for message payloads
    * @param config
    *   JDBC queue configuration
    * @return
    *   a Resource that manages the queue lifecycle
    */
  def apply[A](
      serializer: MessageSerializer[A],
      config: DelayedQueueJDBCConfig
  ): Resource[IO, DelayedQueue[A]] =
    Resource.make(
      IO {
        val jvmQueue = jvm.DelayedQueueJDBC.create(
          serializer.asJava,
          config.asJava
        )
        new DelayedQueueJDBCWrapper(jvmQueue)
      }
    )(queue => IO(queue.underlying.close()))

  /** Runs database migrations for the queue.
    *
    * This should be called once per database before creating queue instances.
    *
    * @tparam A
    *   the type of message payloads
    * @param serializer
    *   serializer for message payloads
    * @param config
    *   JDBC queue configuration
    * @return
    *   IO action that runs the migrations
    */
  def runMigrations[A](
      serializer: MessageSerializer[A],
      config: DelayedQueueJDBCConfig
  ): IO[Unit] =
    IO {
      jvm.DelayedQueueJDBC.runMigrations(
        serializer.asJava,
        config.asJava
      )
    }

  /** Wrapper that implements the Scala DelayedQueue trait by delegating to the JVM implementation.
    */
  private class DelayedQueueJDBCWrapper[A](
      val underlying: jvm.DelayedQueueJDBC[A]
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
