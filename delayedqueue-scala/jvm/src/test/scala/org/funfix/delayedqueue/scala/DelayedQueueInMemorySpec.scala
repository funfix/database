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
import cats.effect.unsafe.implicits.global
import java.time.Instant
import scala.concurrent.duration.*

class DelayedQueueInMemorySpec extends munit.FunSuite {

  test("create should return a working queue") {
    val queue = DelayedQueueInMemory.create[String]()
    val result = queue.getTimeConfig.unsafeRunSync()
    assertEquals(result, DelayedQueueTimeConfig.DEFAULT_IN_MEMORY)
  }

  test("offerOrUpdate should create a new message") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)
    val result = queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    assertEquals(result, OfferOutcome.Created)
  }

  test("offerOrUpdate should update an existing message") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    val result = queue.offerOrUpdate("key1", "payload2", scheduleAt.plusSeconds(5)).unsafeRunSync()

    assertEquals(result, OfferOutcome.Updated)
  }

  test("offerIfNotExists should create a new message") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)
    val result = queue.offerIfNotExists("key1", "payload1", scheduleAt).unsafeRunSync()
    assertEquals(result, OfferOutcome.Created)
  }

  test("offerIfNotExists should ignore existing message") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)

    queue.offerIfNotExists("key1", "payload1", scheduleAt).unsafeRunSync()
    val result =
      queue.offerIfNotExists("key1", "payload2", scheduleAt.plusSeconds(5)).unsafeRunSync()

    assertEquals(result, OfferOutcome.Ignored)
  }

  test("tryPoll should return None when no messages are available") {
    val queue = DelayedQueueInMemory.create[String]()
    val result = queue.tryPoll.unsafeRunSync()
    assertEquals(result, None)
  }

  test("tryPoll should return a message when scheduled time has passed") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().minusSeconds(1)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    val envelope = queue.tryPoll.unsafeRunSync()

    assert(envelope.isDefined)
    assertEquals(envelope.get.payload, "payload1")
    assertEquals(envelope.get.messageId.value, "key1")
  }

  test("tryPollMany should return empty list when no messages are available") {
    val queue = DelayedQueueInMemory.create[String]()
    val envelope = queue.tryPollMany(10).unsafeRunSync()
    assertEquals(envelope.payload, List.empty[String])
  }

  test("tryPollMany should return multiple messages") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().minusSeconds(1)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    queue.offerOrUpdate("key2", "payload2", scheduleAt).unsafeRunSync()
    queue.offerOrUpdate("key3", "payload3", scheduleAt).unsafeRunSync()

    val envelope = queue.tryPollMany(5).unsafeRunSync()

    assertEquals(envelope.payload.length, 3)
    assert(envelope.payload.contains("payload1"))
    assert(envelope.payload.contains("payload2"))
    assert(envelope.payload.contains("payload3"))
  }

  test("offerBatch should handle multiple messages") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)

    val messages = List(
      BatchedMessage("input1", ScheduledMessage("key1", "payload1", scheduleAt, canUpdate = true)),
      BatchedMessage("input2", ScheduledMessage("key2", "payload2", scheduleAt, canUpdate = true))
    )

    val replies = queue.offerBatch(messages).unsafeRunSync()

    assertEquals(replies.length, 2)
    assertEquals(replies(0).outcome, OfferOutcome.Created)
    assertEquals(replies(1).outcome, OfferOutcome.Created)
  }

  test("read should return a message without locking it") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    val envelope = queue.read("key1").unsafeRunSync()

    assert(envelope.isDefined)
    assertEquals(envelope.get.payload, "payload1")

    // Message should still exist
    val stillExists = queue.containsMessage("key1").unsafeRunSync()
    assert(stillExists)
  }

  test("dropMessage should remove a message") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    val dropped = queue.dropMessage("key1").unsafeRunSync()

    assert(dropped)

    val exists = queue.containsMessage("key1").unsafeRunSync()
    assert(!exists)
  }

  test("containsMessage should return true for existing message") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    val exists = queue.containsMessage("key1").unsafeRunSync()

    assert(exists)
  }

  test("containsMessage should return false for non-existing message") {
    val queue = DelayedQueueInMemory.create[String]()
    val exists = queue.containsMessage("nonexistent").unsafeRunSync()
    assert(!exists)
  }

  test("dropAllMessages should remove all messages") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().plusSeconds(10)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    queue.offerOrUpdate("key2", "payload2", scheduleAt).unsafeRunSync()

    val count = queue.dropAllMessages("Yes, please, I know what I'm doing!").unsafeRunSync()

    assertEquals(count, 2)

    val exists1 = queue.containsMessage("key1").unsafeRunSync()
    val exists2 = queue.containsMessage("key2").unsafeRunSync()
    assert(!exists1)
    assert(!exists2)
  }

  test("acknowledge should delete the message") {
    val queue = DelayedQueueInMemory.create[String]()
    val scheduleAt = Instant.now().minusSeconds(1)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    val envelope = queue.tryPoll.unsafeRunSync()

    assert(envelope.isDefined)
    envelope.get.acknowledge.unsafeRunSync()

    // Message should be deleted after acknowledgment
    val exists = queue.containsMessage("key1").unsafeRunSync()
    assert(!exists)
  }

  test("getCron should return a CronService") {
    val queue = DelayedQueueInMemory.create[String]()
    val cronService = queue.getCron.unsafeRunSync()
    assert(cronService != null)
  }

  test("custom timeConfig should be used") {
    val customConfig = DelayedQueueTimeConfig(
      acquireTimeout = 60.seconds,
      pollPeriod = 200.milliseconds
    )
    val queue = DelayedQueueInMemory.create[String](timeConfig = customConfig)
    val result = queue.getTimeConfig.unsafeRunSync()
    assertEquals(result, customConfig)
  }

  test("custom ackEnvSource should be used") {
    val queue = DelayedQueueInMemory.create[String](ackEnvSource = "custom-source")
    val scheduleAt = Instant.now().minusSeconds(1)

    queue.offerOrUpdate("key1", "payload1", scheduleAt).unsafeRunSync()
    val envelope = queue.tryPoll.unsafeRunSync()

    assert(envelope.isDefined)
    assertEquals(envelope.get.source, "custom-source")
  }
}
