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

import cats.syntax.all.*
import cats.effect.IO
import java.time.Instant
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Base test suite for DelayedQueueJDBC with common test cases. */
abstract class DelayedQueueJDBCSpec extends CatsEffectSuite {

  /** Create a queue configuration for testing. Subclasses override this. */
  def createConfig(tableName: String, queueName: String): DelayedQueueJDBCConfig

  /** Helper to create a queue with default settings. */
  def createQueue(tableName: String = "delayed_queue", queueName: String = "test-queue") =
    DelayedQueueJDBC[String](MessageSerializer.forStrings, createConfig(tableName, queueName))

  /** Helper to run migrations and create a queue with unique table names to ensure test isolation.
    */
  def withQueue(test: DelayedQueue[String] => IO[Unit]): IO[Unit] = {
    // Use a unique table name for each test to avoid interference
    val tableName = s"test_table_${System.nanoTime()}"
    val queueName = "test-queue"
    val config = createConfig(tableName, queueName)
    for {
      _ <- DelayedQueueJDBC.runMigrations(config)
      _ <- createQueue(tableName, queueName).use(test)
    } yield ()
  }

  test("offerOrUpdate should create a new message") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        result <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
      } yield assertEquals(result, OfferOutcome.Created)
    }
  }

  test("offerOrUpdate should update an existing message") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        result <- queue.offerOrUpdate("key1", "payload2", scheduleAt.plusSeconds(5))
      } yield assertEquals(result, OfferOutcome.Updated)
    }
  }

  test("offerIfNotExists should create a new message") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        result <- queue.offerIfNotExists("key1", "payload1", scheduleAt)
      } yield assertEquals(result, OfferOutcome.Created)
    }
  }

  test("offerIfNotExists should ignore existing message") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerIfNotExists("key1", "payload1", scheduleAt)
        result <- queue.offerIfNotExists("key1", "payload2", scheduleAt.plusSeconds(5))
      } yield assertEquals(result, OfferOutcome.Ignored)
    }
  }

  test("tryPoll should return None when no messages are available") {
    withQueue { queue =>
      queue.tryPoll.assertEquals(None)
    }
  }

  test("tryPoll should return a message when scheduled time has passed") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().minusSeconds(1))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        envelope <- queue.tryPoll
        _ <- IO {
          assert(envelope.isDefined)
          assertEquals(envelope.get.payload, "payload1")
          assertEquals(envelope.get.messageId.value, "key1")
        }
      } yield ()
    }
  }

  test("tryPollMany should return empty list when no messages are available") {
    withQueue { queue =>
      queue.tryPollMany(10).map { envelope =>
        assertEquals(envelope.payload, List.empty[String])
      }
    }
  }

  test("tryPollMany should return multiple messages") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().minusSeconds(1))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        _ <- queue.offerOrUpdate("key2", "payload2", scheduleAt)
        _ <- queue.offerOrUpdate("key3", "payload3", scheduleAt)
        envelope <- queue.tryPollMany(5)
        _ <- IO {
          assertEquals(envelope.payload.length, 3)
          assertEquals(
            envelope.payload.toSet,
            Set("payload1", "payload2", "payload3"),
            "tryPollMany should return all three messages"
          )
        }
      } yield ()
    }
  }

  test("offerBatch should handle multiple messages") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        messages = List(
          BatchedMessage(
            "input1",
            ScheduledMessage("key1", "payload1", scheduleAt, canUpdate = true)
          ),
          BatchedMessage(
            "input2",
            ScheduledMessage("key2", "payload2", scheduleAt, canUpdate = true)
          )
        )
        replies <- queue.offerBatch(messages)
        _ <- IO {
          assertEquals(replies.length, 2)
          assertEquals(replies(0).outcome, OfferOutcome.Created)
          assertEquals(replies(1).outcome, OfferOutcome.Created)
        }
      } yield ()
    }
  }

  test("read should return a message without locking it") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        envelope <- queue.read("key1")
        stillExists <- queue.containsMessage("key1")
        _ <- IO {
          assert(envelope.isDefined, "envelope should be defined")
          assertEquals(envelope.get.payload, "payload1")
          assert(stillExists, "message should still exist after read")
        }
      } yield ()
    }
  }

  test("dropMessage should remove a message") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        dropped <- queue.dropMessage("key1")
        exists <- queue.containsMessage("key1")
        _ <- IO {
          assert(dropped, "dropMessage should return true")
          assert(!exists, "message should not exist after drop")
        }
      } yield ()
    }
  }

  test("containsMessage should return true for existing message") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        exists <- queue.containsMessage("key1")
        _ <- IO(assert(exists, "message should exist"))
      } yield ()
    }
  }

  test("containsMessage should return false for non-existing message") {
    withQueue { queue =>
      queue.containsMessage("nonexistent").map { exists =>
        assert(!exists, "nonexistent message should not exist")
      }
    }
  }

  test("dropAllMessages should remove all messages") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        _ <- queue.offerOrUpdate("key2", "payload2", scheduleAt)
        count <- queue.dropAllMessages("Yes, please, I know what I'm doing!")
        exists1 <- queue.containsMessage("key1")
        exists2 <- queue.containsMessage("key2")
        _ <- IO {
          assertEquals(count, 2)
          assert(!exists1, "key1 should not exist after dropAll")
          assert(!exists2, "key2 should not exist after dropAll")
        }
      } yield ()
    }
  }

  test("acknowledge should delete the message") {
    withQueue { queue =>
      for {
        scheduleAt <- IO(Instant.now().minusSeconds(1))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        envelope <- queue.tryPoll
        _ <- {
          assert(envelope.isDefined, "envelope should be defined")
          envelope.get.acknowledge
        }
        exists <- queue.containsMessage("key1")
        _ <- IO(assert(!exists, "message should be deleted after acknowledgment"))
      } yield ()
    }
  }

  test("cron should return a CronService") {
    withQueue { queue =>
      queue.cron.map { cronService =>
        assert(cronService != null, "cronService should not be null")
      }
    }
  }

  test("getTimeConfig should return the configured time config") {
    withQueue { queue =>
      queue.getTimeConfig.map { config =>
        assert(config != null, "timeConfig should not be null")
      }
    }
  }

  test("multiple queues can share the same table") {
    val tableName = s"shared_table_${System.nanoTime()}"
    for {
      config1 = createConfig(tableName, "queue1")
      config2 = createConfig(tableName, "queue2")
      _ <- DelayedQueueJDBC.runMigrations(config1)
      _ <- createQueue(tableName, "queue1").use { queue1 =>
        createQueue(tableName, "queue2").use { queue2 =>
          for {
            scheduleAt <- IO(Instant.now().minusSeconds(1))
            _ <- queue1.offerOrUpdate("key1", "queue1-payload", scheduleAt)
            _ <- queue2.offerOrUpdate("key1", "queue2-payload", scheduleAt)
            envelope1 <- queue1.tryPoll
            envelope2 <- queue2.tryPoll
            _ <- IO {
              assert(envelope1.isDefined, "queue1 should have a message")
              assert(envelope2.isDefined, "queue2 should have a message")
              assertEquals(envelope1.get.payload, "queue1-payload")
              assertEquals(envelope2.get.payload, "queue2-payload")
            }
          } yield ()
        }
      }
    } yield ()
  }
}

/** H2 database tests for DelayedQueueJDBC. */
class DelayedQueueJDBCH2Spec extends DelayedQueueJDBCSpec {
  // Use a stable database name for the test suite
  private val testDbName = s"test_h2_${System.currentTimeMillis()}"

  override def createConfig(tableName: String, queueName: String): DelayedQueueJDBCConfig = {
    val dbConfig = JdbcConnectionConfig(
      url = s"jdbc:h2:mem:$testDbName;DB_CLOSE_DELAY=-1",
      driver = JdbcDriver.H2,
      username = Some("sa"),
      password = Some("")
    )
    DelayedQueueJDBCConfig.create(dbConfig, tableName, queueName)
  }
}

/** SQLite database tests for DelayedQueueJDBC. */
class DelayedQueueJDBCSQLiteSpec extends DelayedQueueJDBCSpec {
  // SQLite needs special mode for shared cache in-memory database
  private val testDbName = s"test_sqlite_${System.currentTimeMillis()}"

  override def createConfig(tableName: String, queueName: String): DelayedQueueJDBCConfig = {
    val dbConfig = JdbcConnectionConfig(
      url = s"jdbc:sqlite:file:$testDbName?mode=memory&cache=shared",
      driver = JdbcDriver.Sqlite
    )
    DelayedQueueJDBCConfig.create(dbConfig, tableName, queueName)
  }
}

/** HSQLDB database tests for DelayedQueueJDBC. */
class DelayedQueueJDBCHSQLDBSpec extends DelayedQueueJDBCSpec {
  private val testDbName = s"test_hsqldb_${System.currentTimeMillis()}"

  override def createConfig(tableName: String, queueName: String): DelayedQueueJDBCConfig = {
    val dbConfig = JdbcConnectionConfig(
      url = s"jdbc:hsqldb:mem:$testDbName",
      driver = JdbcDriver.HSQLDB,
      username = Some("SA"),
      password = Some("")
    )
    DelayedQueueJDBCConfig.create(dbConfig, tableName, queueName)
  }
}
