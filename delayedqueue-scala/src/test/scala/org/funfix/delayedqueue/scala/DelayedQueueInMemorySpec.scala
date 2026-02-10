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
import cats.syntax.all.*
import java.time.Instant
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class DelayedQueueInMemorySpec extends CatsEffectSuite {

  test("apply should return a working queue") {
    DelayedQueueInMemory[String]().use { queue =>
      queue.getTimeConfig.assertEquals(DelayedQueueTimeConfig.DEFAULT_IN_MEMORY)
    }
  }

  test("offerOrUpdate should create a new message") {
    DelayedQueueInMemory[String]().use { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        result <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
      } yield assertEquals(result, OfferOutcome.Created)
    }
  }

  test("offerOrUpdate should update an existing message") {
    DelayedQueueInMemory[String]().use { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        result <- queue.offerOrUpdate("key1", "payload2", scheduleAt.plusSeconds(5))
      } yield assertEquals(result, OfferOutcome.Updated)
    }
  }

  test("offerIfNotExists should create a new message") {
    DelayedQueueInMemory[String]().use { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        result <- queue.offerIfNotExists("key1", "payload1", scheduleAt)
      } yield assertEquals(result, OfferOutcome.Created)
    }
  }

  test("offerIfNotExists should ignore existing message") {
    DelayedQueueInMemory[String]().use { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerIfNotExists("key1", "payload1", scheduleAt)
        result <- queue.offerIfNotExists("key1", "payload2", scheduleAt.plusSeconds(5))
      } yield assertEquals(result, OfferOutcome.Ignored)
    }
  }

  test("tryPoll should return None when no messages are available") {
    DelayedQueueInMemory[String]().use { queue =>
      queue.tryPoll.assertEquals(None)
    }
  }

  test("tryPoll should return a message when scheduled time has passed") {
    DelayedQueueInMemory[String]().use { queue =>
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
    DelayedQueueInMemory[String]().use { queue =>
      queue.tryPollMany(10).map { envelope =>
        assertEquals(envelope.payload, List.empty[String])
      }
    }
  }

  test("tryPollMany should return multiple messages") {
    DelayedQueueInMemory[String]().use { queue =>
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
    DelayedQueueInMemory[String]().use { queue =>
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
    DelayedQueueInMemory[String]().use { queue =>
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
    DelayedQueueInMemory[String]().use { queue =>
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
    DelayedQueueInMemory[String]().use { queue =>
      for {
        scheduleAt <- IO(Instant.now().plusSeconds(10))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        exists <- queue.containsMessage("key1")
        _ <- IO(assert(exists, "message should exist"))
      } yield ()
    }
  }

  test("containsMessage should return false for non-existing message") {
    DelayedQueueInMemory[String]().use { queue =>
      queue.containsMessage("nonexistent").map { exists =>
        assert(!exists, "nonexistent message should not exist")
      }
    }
  }

  test("dropAllMessages should remove all messages") {
    DelayedQueueInMemory[String]().use { queue =>
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
    DelayedQueueInMemory[String]().use { queue =>
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
    DelayedQueueInMemory[String]().use { queue =>
      queue.cron.map { cronService =>
        assert(cronService != null, "cronService should not be null")
      }
    }
  }

  test("custom timeConfig should be used") {
    val customConfig = DelayedQueueTimeConfig(
      acquireTimeout = 60.seconds,
      pollPeriod = 200.milliseconds
    )
    DelayedQueueInMemory[String](timeConfig = customConfig).use { queue =>
      queue.getTimeConfig.assertEquals(customConfig)
    }
  }

  test("custom ackEnvSource should be used") {
    DelayedQueueInMemory[String](ackEnvSource = "custom-source").use { queue =>
      for {
        scheduleAt <- IO(Instant.now().minusSeconds(1))
        _ <- queue.offerOrUpdate("key1", "payload1", scheduleAt)
        envelope <- queue.tryPoll
        _ <- IO {
          assert(envelope.isDefined, "envelope should be defined")
          assertEquals(envelope.get.source, "custom-source")
        }
      } yield ()
    }
  }

  test("time passage: offer in future, tryPoll returns None, advance time, tryPoll succeeds") {
    DelayedQueueInMemory[String]().use { queue =>
      for {
        now <- IO.realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        futureTime = now.plusMillis(100) // 100ms in the future
        pastTime = now.minusMillis(100) // 100ms in the past

        // Offer a message scheduled for the future
        _ <- queue.offerOrUpdate("key1", "payload1", futureTime)

        // Try to poll immediately - should get None (not scheduled yet)
        resultBefore <- queue.tryPoll

        // Offer a message scheduled for the past
        _ <- queue.offerOrUpdate("key2", "payload2", pastTime)

        // Now tryPoll should succeed on the past-scheduled message
        resultAfter <- queue.tryPoll

        _ <- IO {
          assertEquals(resultBefore, None, "tryPoll should return None before scheduled time")
          assert(resultAfter.isDefined, "tryPoll should return Some for past-scheduled message")
          assertEquals(resultAfter.get.payload, "payload2")
          assertEquals(resultAfter.get.messageId.value, "key2")
        }
      } yield ()
    }
  }

  test("concurrency") {
    val producers = 4
    val consumers = 4
    val messageCount = 10000
    val now = Instant.now()

    assert(messageCount % producers == 0, "messageCount should be divisible by number of producers")
    assert(messageCount % consumers == 0, "messageCount should be divisible by number of producers")

    def producer(queue: DelayedQueue[String], id: Int, count: Int): IO[Int] =
      (1 to count).toList.traverse { i =>
        val key = s"producer-$id-message-$i"
        val payload = s"payload-$key"
        queue.offerOrUpdate(key, payload, now).map { outcome =>
          assertEquals(outcome, OfferOutcome.Created)
          1
        }
      }.map {
        _.sum
      }

    def allProducers(queue: DelayedQueue[String]): IO[Int] = (1 to producers).toList.parTraverse {
      id =>
        producer(queue, id, messageCount / producers)
    }.map {
      _.sum
    }

    def consumer(queue: DelayedQueue[String], count: Int): IO[Int] = (1 to count).toList.traverse {
      _ =>
        queue.poll.map { envelope =>
          assert(
            envelope.payload.startsWith("payload-"),
            "payload should have correct format"
          )
          1
        }
    }.map {
      _.sum
    }

    def allConsumers(queue: DelayedQueue[String]): IO[Int] = (1 to consumers).toList.parTraverse {
      _ =>
        consumer(queue, messageCount / consumers)
    }.map {
      _.sum
    }

    val res =
      for {
        queue <- DelayedQueueInMemory[String]()
        prodFiber <- allProducers(queue).background
        conFiber <- allConsumers(queue).background
      } yield (prodFiber, conFiber)

    res.use {
      case (prodFiber, conFiber) =>
        for {
          p <- prodFiber
          p <- p.embedNever
          c <- conFiber
          c <- c.embedNever
        } yield {
          assertEquals(p, messageCount)
          assertEquals(c, messageCount)
        }
    }.timeout(30.seconds)
  }
}
