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

@file:Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")

package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.util.Collections

/**
 * Configuration for daily recurring scheduled messages with timezone support.
 *
 * This class defines when messages should be scheduled each day, with support for multiple times
 * per day and scheduling messages in advance.
 *
 * @property zoneId the timezone for interpreting the hours of day
 * @property hoursOfDay the times during each day when messages should be scheduled (must not be
 *   empty)
 * @property scheduleInAdvance how far in advance to schedule messages
 * @property scheduleInterval how often to check and update the schedule
 */
@JvmRecord
public data class CronDailySchedule(
    val zoneId: ZoneId,
    val hoursOfDay: List<LocalTime>,
    val scheduleInAdvance: Duration,
    val scheduleInterval: Duration,
) {
    init {
        require(hoursOfDay.isNotEmpty()) { "hoursOfDay must not be empty" }
        require(!scheduleInterval.isZero && !scheduleInterval.isNegative) {
            "scheduleInterval must be positive"
        }
    }

    /**
     * Calculates the next scheduled times starting from now.
     *
     * Returns all times that should be scheduled, from now until (now + scheduleInAdvance). Always
     * returns at least one time (the next scheduled time), even if it's beyond scheduleInAdvance.
     *
     * @param now the current time
     * @return list of future instants when messages should be scheduled (never empty)
     */
    public fun getNextTimes(now: Instant): List<Instant> {
        val until = now.plus(scheduleInAdvance)
        val sortedHours = hoursOfDay.sortedBy { it }
        val result = java.util.ArrayList<Instant>()

        var currentTime = now
        var nextTime = getNextTime(currentTime, sortedHours)

        // Always add the first nextTime (matches NonEmptyList behavior from Scala)
        result.add(nextTime)

        // Then add more if they're within the window
        while (true) {
            currentTime = nextTime
            nextTime = getNextTime(currentTime, sortedHours)
            if (nextTime.isAfter(until)) {
                break
            }
            result.add(nextTime)
        }

        return Collections.unmodifiableList(result)
    }

    private fun getNextTime(now: Instant, sortedHours: List<LocalTime>): Instant {
        val zonedDateTime = now.atZone(zoneId)
        val localNow = zonedDateTime.toLocalTime()

        // Find the next hour today
        val nextHourToday = sortedHours.firstOrNull { it.isAfter(localNow) }

        return if (nextHourToday != null) {
            // Schedule for today
            nextHourToday.atDate(zonedDateTime.toLocalDate()).atZone(zoneId).toInstant()
        } else {
            // Schedule for tomorrow (first hour of the day)
            sortedHours
                .first()
                .atDate(zonedDateTime.toLocalDate().plusDays(1))
                .atZone(zoneId)
                .toInstant()
        }
    }

    public companion object {
        /** Creates a DailyCronSchedule with the specified configuration. */
        @JvmStatic
        public fun create(
            zoneId: ZoneId,
            hoursOfDay: List<LocalTime>,
            scheduleInAdvance: Duration,
            scheduleInterval: Duration,
        ): CronDailySchedule =
            CronDailySchedule(zoneId, hoursOfDay, scheduleInAdvance, scheduleInterval)
    }
}
