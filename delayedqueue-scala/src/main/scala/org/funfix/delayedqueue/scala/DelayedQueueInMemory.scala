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

import cats.effect.Clock
import cats.effect.IO
import cats.effect.Resource
import cats.effect.std.Dispatcher
import java.time.Clock as JavaClock
import java.time.Instant
import org.funfix.delayedqueue.jvm

/** In-memory implementation of [[DelayedQueue]] using concurrent data
  * structures.
  *
  * This implementation wraps the JVM
  * [[org.funfix.delayedqueue.jvm.DelayedQueueInMemory]] and provides an
  * idiomatic Scala API with Cats Effect IO for managing side effects.
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
  */
object DelayedQueueInMemory {

  /** Creates an in-memory delayed queue with default configuration.
    *
    * @tparam A
    *   the type of message payloads
    * @param timeConfig
    *   time configuration (defaults to
    *   [[DelayedQueueTimeConfig.DEFAULT_IN_MEMORY]])
    * @param ackEnvSource
    *   source identifier for envelopes (defaults to "delayed-queue-inmemory")
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
        new DelayedQueueWrapper(jvmQueue)
      }
    }
}

final private[scala] class CatsClockToJavaClock(
  dispatcher: Dispatcher[IO],
  zone: java.time.ZoneId = java.time.ZoneId.systemDefault()
)(implicit clock: Clock[IO]) extends JavaClock {
  override def getZone: java.time.ZoneId =
    zone

  override def withZone(zone: java.time.ZoneId): JavaClock =
    new CatsClockToJavaClock(dispatcher, zone)

  override def instant(): Instant =
    dispatcher.unsafeRunSync(
      Clock[IO].realTime.map(it => Instant.ofEpochMilli(it.toMillis))
    )
}

private[scala] object CatsClockToJavaClock {
  def apply(dispatcher: Dispatcher[IO])(implicit clock: Clock[IO]): CatsClockToJavaClock =
    new CatsClockToJavaClock(dispatcher)
}
