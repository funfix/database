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
     * Returns all times that should be scheduled, from now until (now + scheduleInAdvance).
     *
     * @param now the current time
     * @return list of future instants when messages should be scheduled
     */
    public fun getNextTimes(now: Instant): List<Instant> {
        val until = now.plus(scheduleInAdvance)
        val sortedHours = hoursOfDay.sortedBy { it }
        val result = java.util.ArrayList<Instant>()

        var currentTime = now
        var nextTime = getNextTime(currentTime, sortedHours)

        if (!nextTime.isAfter(until)) {
            result.add(nextTime)
            while (true) {
                currentTime = nextTime
                nextTime = getNextTime(currentTime, sortedHours)
                if (nextTime.isAfter(until)) {
                    break
                }
                result.add(nextTime)
            }
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
