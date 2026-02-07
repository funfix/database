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

package org.funfix.delayedqueue.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import org.funfix.delayedqueue.jvm.CronConfigHash;
import org.funfix.delayedqueue.jvm.CronDailySchedule;
import org.junit.jupiter.api.Test;

public class CronDailyScheduleTest {

    @Test
    public void create_rejectsEmptyHours() {
        assertThrows(IllegalArgumentException.class, () -> CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(),
            Duration.ofHours(1),
            Duration.ofSeconds(1)
        ));
    }

    @Test
    public void create_rejectsNonPositiveScheduleInterval() {
        assertThrows(IllegalArgumentException.class, () -> CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofHours(1),
            Duration.ZERO
        ));
    }

    @Test
    public void getNextTimes_calculatesCorrectly() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00"), LocalTime.parse("18:00:00")),
            Duration.ofHours(3),
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        assertFalse(nextTimes.isEmpty());
        assertEquals(Instant.parse("2024-01-01T12:00:00Z"), nextTimes.getFirst());
    }

    @Test
    public void getNextTimes_alwaysReturnsAtLeastOne() {
        // This matches the original Scala behavior (NonEmptyList)
        // Even if the next time is beyond scheduleInAdvance, it should be included
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofMinutes(30),  // scheduleInAdvance too short
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        // Should still return the next scheduled time even though it's beyond scheduleInAdvance
        assertFalse(nextTimes.isEmpty());
        assertEquals(Instant.parse("2024-01-01T12:00:00Z"), nextTimes.getFirst());
    }

    @Test
    public void getNextTimes_withScheduleInAdvance_schedulesMultipleDays() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(2),
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        assertTrue(nextTimes.size() >= 2);
        assertTrue(nextTimes.contains(Instant.parse("2024-01-01T12:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-02T12:00:00Z")));
    }

    @Test
    public void getNextTimes_skipsCurrentHour() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00"), LocalTime.parse("18:00:00")),
            Duration.ofHours(7),
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T12:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        assertEquals(1, nextTimes.size());
        assertEquals(Instant.parse("2024-01-01T18:00:00Z"), nextTimes.getFirst());
    }

    @Test
    public void getNextTimes_schedulesNextDay() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00"), LocalTime.parse("18:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T20:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        assertEquals(2, nextTimes.size());
        assertEquals(Instant.parse("2024-01-02T12:00:00Z"), nextTimes.getFirst());
        assertEquals(Instant.parse("2024-01-02T18:00:00Z"), nextTimes.getLast());
    }

    @Test
    public void getNextTimes_withMultipleHoursPerDay() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(
                LocalTime.parse("09:00:00"),
                LocalTime.parse("12:00:00"),
                LocalTime.parse("18:00:00")
            ),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        assertTrue(nextTimes.size() >= 3);
        assertTrue(nextTimes.contains(Instant.parse("2024-01-01T12:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-01T18:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-02T09:00:00Z")));
    }

    @Test
    public void getNextTimes_sortsHoursOfDay() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("18:00:00"), LocalTime.parse("12:00:00")),
            Duration.ofHours(12),
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        assertEquals(Instant.parse("2024-01-01T12:00:00Z"), nextTimes.getFirst());
    }

    @Test
    public void configHash_fromDailyCron_isDeterministic() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("Europe/Amsterdam"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );

        var hash1 = CronConfigHash.fromDailyCron(schedule);
        var hash2 = CronConfigHash.fromDailyCron(schedule);

        assertEquals(hash1, hash2);
    }

    @Test
    public void configHash_fromDailyCron_changesWithDifferentTimezone() {
        var schedule1 = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );
        var schedule2 = CronDailySchedule.create(
            ZoneId.of("America/New_York"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );

        var hash1 = CronConfigHash.fromDailyCron(schedule1);
        var hash2 = CronConfigHash.fromDailyCron(schedule2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    public void configHash_fromDailyCron_changesWithDifferentHours() {
        var schedule1 = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );
        var schedule2 = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("18:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );

        var hash1 = CronConfigHash.fromDailyCron(schedule1);
        var hash2 = CronConfigHash.fromDailyCron(schedule2);

        assertNotEquals(hash1, hash2);
    }
}
