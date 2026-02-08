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

import munit.ScalaCheckSuite
import org.scalacheck.Prop.*
import org.scalacheck.Gen
import scala.concurrent.duration.FiniteDuration
import java.time.{Instant, LocalTime, ZoneId}
import org.funfix.delayedqueue.scala.ScheduledMessage.asScala as scheduledAsScala
import org.funfix.delayedqueue.scala.DelayedQueueTimeConfig.asScala as timeConfigAsScala
import org.funfix.delayedqueue.scala.MessageId.asScala as messageIdAsScala
import org.funfix.delayedqueue.scala.OfferOutcome.asScala as offerOutcomeAsScala
import org.funfix.delayedqueue.scala.Generators.{given, *}

class DataStructuresPropertySpec extends ScalaCheckSuite {
  property("ScheduledMessage asJava/asScala roundtrip preserves data") {
    forAll { (key: String, payload: String, instant: Instant, canUpdate: Boolean) =>
      val original = ScheduledMessage(key, payload, instant, canUpdate)
      val roundtripped = original.asJava.scheduledAsScala

      assertEquals(roundtripped.key, original.key)
      assertEquals(roundtripped.payload, original.payload)
      assertEquals(roundtripped.scheduleAt, original.scheduleAt)
      assertEquals(roundtripped.canUpdate, original.canUpdate)
    }
  }

  property("DelayedQueueTimeConfig asJava/asScala roundtrip preserves data") {
    forAll { (acquireTimeout: FiniteDuration, pollPeriod: FiniteDuration) =>
      val original = DelayedQueueTimeConfig(acquireTimeout, pollPeriod)
      val roundtripped = original.asJava.timeConfigAsScala

      assertEquals(roundtripped.acquireTimeout.toMillis, original.acquireTimeout.toMillis)
      assertEquals(roundtripped.pollPeriod.toMillis, original.pollPeriod.toMillis)
    }
  }

  property("MessageId is symmetric") {
    forAll { (value: String) =>
      val messageId = MessageId(value)
      assertEquals(messageId.value, value)
      assertEquals(messageId.asJava.messageIdAsScala.value, value)
    }
  }

  property("CronConfigHash fromString is deterministic") {
    forAll { (input: String) =>
      val hash1 = CronConfigHash.fromString(input)
      val hash2 = CronConfigHash.fromString(input)
      assertEquals(hash1.value, hash2.value)
    }
  }

  property("CronMessage key is unique for different times") {
    forAll { (instant1: Instant, instant2: Instant) =>
      if instant1 != instant2 then {
        val hash = CronConfigHash.fromString("test")
        val prefix = "test-prefix"
        val key1 = CronMessage.key(hash, prefix, instant1)
        val key2 = CronMessage.key(hash, prefix, instant2)
        assertNotEquals(key1, key2)
      }
    }
  }

  property("BatchedMessage covariance works correctly") {
    forAll { (input: Int, payload: String, instant: Instant) =>
      val message = ScheduledMessage(s"key-$input", payload, instant)
      val batched: BatchedMessage[Int, String] = BatchedMessage(input, message)
      // Covariance allows us to upcast
      val widened: BatchedMessage[Any, Any] = batched
      assertEquals(widened.input, input)
    }
  }

  property("BatchedReply covariance works correctly") {
    forAll { (input: Int, payload: String, instant: Instant) =>
      val message = ScheduledMessage(s"key-$input", payload, instant)
      val reply: BatchedReply[Int, String] = BatchedReply(input, message, OfferOutcome.Created)
      // Covariance allows us to upcast
      val widened: BatchedReply[Any, Any] = reply
      assertEquals(widened.input, input)
    }
  }

  property("CronDailySchedule getNextTimes always returns at least one time") {
    forAll { (hours: List[LocalTime], now: Instant, zoneId: ZoneId) =>
      if hours.nonEmpty then {
        val schedule = CronDailySchedule(
          zoneId = zoneId,
          hoursOfDay = hours,
          scheduleInAdvance = java.time.Duration.ofDays(1),
          scheduleInterval = java.time.Duration.ofHours(1)
        )
        val nextTimes = schedule.getNextTimes(now)
        assert(nextTimes.nonEmpty, "getNextTimes should return at least one time")
        assert(nextTimes.head.isAfter(now) || nextTimes.head == now, "First time should be >= now")
      }
    }
  }

  property("OfferOutcome asJava/asScala roundtrip") {
    forAll(Gen.oneOf(OfferOutcome.Created, OfferOutcome.Updated, OfferOutcome.Ignored)) {
      outcome =>
        val roundtripped = outcome.asJava.offerOutcomeAsScala
        assertEquals(roundtripped, outcome)
    }
  }

  property("DeliveryType asJava/asScala roundtrip") {
    import org.funfix.delayedqueue.scala.DeliveryType.asScala
    forAll(Gen.oneOf(DeliveryType.FirstDelivery, DeliveryType.Redelivery)) { deliveryType =>
      val roundtripped = deliveryType.asJava.asScala
      assertEquals(roundtripped, deliveryType)
    }
  }

  property("RetryConfig validates backoffFactor >= 1.0") {
    forAll { (backoffFactor: Double) =>
      // Simplified test: just check that invalid backoff factors are rejected
      if backoffFactor < 1.0 && !backoffFactor.isNaN && !backoffFactor.isInfinite then {
        val _ = intercept[IllegalArgumentException] {
          RetryConfig(
            initialDelay = java.time.Duration.ofMillis(100),
            maxDelay = java.time.Duration.ofMillis(1000),
            backoffFactor = backoffFactor
          )
        }
        ()
      } else if backoffFactor >= 1.0 && !backoffFactor.isNaN && !backoffFactor.isInfinite then {
        val config = RetryConfig(
          initialDelay = java.time.Duration.ofMillis(100),
          maxDelay = java.time.Duration.ofMillis(1000),
          backoffFactor = backoffFactor
        )
        assert(config.backoffFactor >= 1.0)
      }
    }
  }

  property("JdbcDriver fromClassName is case-insensitive") {
    forAll(Gen.oneOf(JdbcDriver.entries)) { driver =>
      val lower = JdbcDriver.fromClassName(driver.className.toLowerCase)
      val upper = JdbcDriver.fromClassName(driver.className.toUpperCase)
      assertEquals(lower, Some(driver))
      assertEquals(upper, Some(driver))
    }
  }
}
