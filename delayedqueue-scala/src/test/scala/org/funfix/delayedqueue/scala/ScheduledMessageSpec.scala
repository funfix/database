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

import java.time.Instant

class ScheduledMessageSpec extends munit.FunSuite {
  test("ScheduledMessage asJava and fromJava should be symmetric") {
    val original = ScheduledMessage(
      key = "test-key",
      payload = "test-payload",
      scheduleAt = Instant.ofEpochMilli(1000),
      canUpdate = true
    )

    val roundTripped = ScheduledMessage.fromJava(original.asJava)
    assertEquals(roundTripped.key, original.key)
    assertEquals(roundTripped.payload, original.payload)
    assertEquals(roundTripped.scheduleAt, original.scheduleAt)
    assertEquals(roundTripped.canUpdate, original.canUpdate)
  }

  test("BatchedMessage reply should create BatchedReply") {
    val message = ScheduledMessage(
      key = "test-key",
      payload = "test-payload",
      scheduleAt = Instant.ofEpochMilli(1000)
    )

    val batched = BatchedMessage(input = 42, message = message)
    val reply = batched.reply(OfferOutcome.Created)

    assertEquals(reply.input, 42)
    assertEquals(reply.message, message)
    assertEquals(reply.outcome, OfferOutcome.Created)
  }

  test("OfferOutcome isIgnored should work correctly") {
    assert(OfferOutcome.Ignored.isIgnored)
    assert(!OfferOutcome.Created.isIgnored)
    assert(!OfferOutcome.Updated.isIgnored)
  }

  test("OfferOutcome asJava and fromJava should be symmetric") {
    val outcomes = List(OfferOutcome.Created, OfferOutcome.Updated, OfferOutcome.Ignored)

    outcomes.foreach { outcome =>
      val roundTripped = OfferOutcome.fromJava(outcome.asJava)
      assertEquals(roundTripped, outcome)
    }
  }
}
