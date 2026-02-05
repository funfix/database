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
    public void getNextTimes_respectsScheduleInAdvance() {
        var schedule = CronDailySchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofMinutes(30),
            Duration.ofSeconds(1)
        );

        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);

        assertTrue(nextTimes.isEmpty());
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
