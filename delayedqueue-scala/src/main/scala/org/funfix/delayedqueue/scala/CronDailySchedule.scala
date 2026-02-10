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

import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import org.funfix.delayedqueue.jvm
import scala.jdk.CollectionConverters.*

/** Configuration for daily recurring scheduled messages with timezone support.
  *
  * This class defines when messages should be scheduled each day, with support
  * for multiple times per day and scheduling messages in advance.
  *
  * @param zoneId
  *   the timezone for interpreting the hours of day
  * @param hoursOfDay
  *   the times during each day when messages should be scheduled (must not be
  *   empty)
  * @param scheduleInAdvance
  *   how far in advance to schedule messages
  * @param scheduleInterval
  *   how often to check and update the schedule
  */
final case class CronDailySchedule(
  zoneId: ZoneId,
  hoursOfDay: List[LocalTime],
  scheduleInAdvance: Duration,
  scheduleInterval: Duration
) {
  require(hoursOfDay.nonEmpty, "hoursOfDay must not be empty")
  require(
    !scheduleInterval.isZero && !scheduleInterval.isNegative,
    "scheduleInterval must be positive"
  )

  /** Calculates the next scheduled times starting from now.
    *
    * Returns all times that should be scheduled, from now until (now +
    * scheduleInAdvance). Always returns at least one time (the next scheduled
    * time), even if it's beyond scheduleInAdvance.
    *
    * @param now
    *   the current time
    * @return
    *   list of future instants when messages should be scheduled (never empty)
    */
  def getNextTimes(now: Instant): List[Instant] = {
    import scala.jdk.CollectionConverters.*
    asJava.getNextTimes(now).asScala.toList
  }

  /** Converts this Scala CronDailySchedule to a JVM CronDailySchedule. */
  def asJava: jvm.CronDailySchedule =
    new jvm.CronDailySchedule(
      zoneId,
      hoursOfDay.asJava,
      scheduleInAdvance,
      scheduleInterval
    )
}

object CronDailySchedule {

  /** Creates a [[DailyCronSchedule]] with the specified configuration. */
  def create(
    zoneId: ZoneId,
    hoursOfDay: List[LocalTime],
    scheduleInAdvance: Duration,
    scheduleInterval: Duration
  ): CronDailySchedule =
    CronDailySchedule(zoneId, hoursOfDay, scheduleInAdvance, scheduleInterval)

  /** Converts from the JVM type to the Scala-specific [[CronDailySchedule]]. */
  def fromJava(javaSchedule: jvm.CronDailySchedule): CronDailySchedule =
    CronDailySchedule(
      zoneId = javaSchedule.zoneId,
      hoursOfDay = javaSchedule.hoursOfDay.asScala.toList,
      scheduleInAdvance = javaSchedule.scheduleInAdvance,
      scheduleInterval = javaSchedule.scheduleInterval
    )
}
