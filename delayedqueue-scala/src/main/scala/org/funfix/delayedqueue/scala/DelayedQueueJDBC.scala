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
import cats.effect.std.Dispatcher
import org.funfix.delayedqueue.jvm

/** JDBC-based implementation of [[DelayedQueue]] with support for multiple
  * database backends.
  *
  * This implementation wraps the JVM
  * [[org.funfix.delayedqueue.jvm.DelayedQueueJDBC]] and provides an idiomatic
  * Scala API with Cats Effect IO for managing side effects.
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
  * DelayedQueueJDBC.runMigrations(config).unsafeRunSync()
  *
  * // Create and use the queue (using implicit PayloadCodec.forStrings)
  * DelayedQueueJDBC[String](config).use { queue =>
  *   for {
  *     _ <- queue.offerOrUpdate("key1", "Hello", Instant.now().plusSeconds(10))
  *     envelope <- queue.poll
  *     _ <- IO.println(s"Received: $${envelope.payload}")
  *     _ <- envelope.acknowledge
  *   } yield ()
  * }
  * }}}
  */
object DelayedQueueJDBC {

  /** Creates a JDBC-based delayed queue with the given configuration.
    *
    * @tparam A
    *   the type of message payloads, with a [[PayloadCodec]] available for
    *   serialization
    * @param config
    *   JDBC queue configuration
    * @return
    *   a Resource that manages the queue lifecycle
    */
  def apply[A: PayloadCodec](
    config: DelayedQueueJDBCConfig
  ): Resource[IO, DelayedQueue[A]] =
    Dispatcher.sequential[IO].flatMap { dispatcher =>
      Resource.fromAutoCloseable(IO {
        val javaClock = CatsClockToJavaClock(dispatcher)
        jvm.DelayedQueueJDBC.create[A](
          implicitly[PayloadCodec[A]].asJava,
          config.asJava,
          javaClock
        )
      }).map(jvmQueue => new DelayedQueueWrapper(jvmQueue))
    }

  /** Runs database migrations for the queue.
    *
    * This should be called once per database before creating queue instances.
    *
    * @param config
    *   JDBC queue configuration
    * @return
    *   IO action that runs the migrations
    */
  def runMigrations(
    config: DelayedQueueJDBCConfig
  ): IO[Unit] =
    IO.interruptible {
      jvm.DelayedQueueJDBC.runMigrations(
        config.asJava
      )
    }
}
