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
import java.time.LocalTime
import java.time.ZoneId
import java.time.Duration
import org.funfix.delayedqueue.scala.CronConfigHash.asScala
import org.funfix.delayedqueue.scala.CronMessage.asScala as cronAsScala
import org.funfix.delayedqueue.scala.CronDailySchedule.asScala as scheduleAsScala

class CronSpec extends munit.FunSuite {

  test("CronConfigHash fromString should be deterministic") {
    val text = "test-config"
    val hash1 = CronConfigHash.fromString(text)
    val hash2 = CronConfigHash.fromString(text)

    assertEquals(hash1.value, hash2.value)
  }

  test("CronConfigHash fromDailyCron should create hash") {
    val schedule = CronDailySchedule(
      zoneId = ZoneId.of("UTC"),
      hoursOfDay = List(LocalTime.of(10, 0)),
      scheduleInAdvance = Duration.ofDays(1),
      scheduleInterval = Duration.ofHours(1)
    )

    val hash = CronConfigHash.fromDailyCron(schedule)
    assert(hash.value.nonEmpty)
  }

  test("CronConfigHash fromPeriodicTick should create hash") {
    val hash = CronConfigHash.fromPeriodicTick(Duration.ofMinutes(5))
    assert(hash.value.nonEmpty)
  }

  test("CronMessage key should be unique for different times") {
    val hash = CronConfigHash.fromString("test")
    val prefix = "test-prefix"

    val key1 = CronMessage.key(hash, prefix, Instant.ofEpochMilli(1000))
    val key2 = CronMessage.key(hash, prefix, Instant.ofEpochMilli(2000))

    assertNotEquals(key1, key2)
  }

  test("CronMessage toScheduled should create ScheduledMessage") {
    val cronMsg = CronMessage(
      payload = "test-payload",
      scheduleAt = Instant.ofEpochMilli(1000)
    )

    val hash = CronConfigHash.fromString("test")
    val scheduled = cronMsg.toScheduled(hash, "prefix", canUpdate = true)

    assertEquals(scheduled.payload, "test-payload")
    assertEquals(scheduled.scheduleAt, Instant.ofEpochMilli(1000))
    assertEquals(scheduled.canUpdate, true)
  }

  test("CronMessage staticPayload should create generator") {
    val generator = CronMessage.staticPayload("static-payload")
    val instant = Instant.ofEpochMilli(1000)
    val cronMsg = generator(instant)

    assertEquals(cronMsg.payload, "static-payload")
    assertEquals(cronMsg.scheduleAt, instant)
  }

  test("CronDailySchedule getNextTimes should return at least one time") {
    val schedule = CronDailySchedule(
      zoneId = ZoneId.of("UTC"),
      hoursOfDay = List(LocalTime.of(10, 0)),
      scheduleInAdvance = Duration.ofDays(1),
      scheduleInterval = Duration.ofHours(1)
    )

    val now = Instant.parse("2024-01-01T08:00:00Z")
    val nextTimes = schedule.getNextTimes(now)

    assert(nextTimes.nonEmpty)
  }

  test("CronDailySchedule should validate non-empty hours") {
    intercept[IllegalArgumentException] {
      CronDailySchedule(
        zoneId = ZoneId.of("UTC"),
        hoursOfDay = List.empty,
        scheduleInAdvance = Duration.ofDays(1),
        scheduleInterval = Duration.ofHours(1)
      )
    }
  }

  test("CronDailySchedule should validate positive schedule interval") {
    intercept[IllegalArgumentException] {
      CronDailySchedule(
        zoneId = ZoneId.of("UTC"),
        hoursOfDay = List(LocalTime.of(10, 0)),
        scheduleInAdvance = Duration.ofDays(1),
        scheduleInterval = Duration.ZERO
      )
    }
  }
}
