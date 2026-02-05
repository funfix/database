package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Java tests for CronService to ensure the Java API is ergonomic.
 * All tests use TestClock for fast, deterministic execution.
 */
public class CronServiceTest {

    @Test
    public void installTick_createsMessagesInQueue() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var messages = List.of(
            new CronMessage<>("msg1", clock.now().plusSeconds(10)),
            new CronMessage<>("msg2", clock.now().plusSeconds(20))
        );
        
        queue.getCron().installTick(configHash, "cron-prefix-", messages);
        
        // Messages should be available when time advances
        clock.advance(Duration.ofSeconds(15));
        var envelope1 = queue.tryPoll();
        assertNotNull(envelope1);
        assertEquals("msg1", envelope1.getPayload());
        envelope1.acknowledge();
        
        clock.advance(Duration.ofSeconds(10));
        var envelope2 = queue.tryPoll();
        assertNotNull(envelope2);
        assertEquals("msg2", envelope2.getPayload());
        envelope2.acknowledge();
    }
    
    @Test
    public void uninstallTick_removesMessagesFromQueue() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var messages = List.of(
            new CronMessage<>("msg1", clock.now().plusSeconds(10))
        );
        
        queue.getCron().installTick(configHash, "cron-prefix-", messages);
        
        // Key format is: keyPrefix/configHash/timestamp
        var expectedKey = CronMessage.key(configHash, "cron-prefix-", clock.now().plusSeconds(10));
        assertTrue(queue.containsMessage(expectedKey));
        
        queue.getCron().uninstallTick(configHash, "cron-prefix-");
        assertFalse(queue.containsMessage(expectedKey));
    }
    
    @Test
    public void installTick_deletesOldMessagesWithSamePrefix() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        // Install first set of messages
        queue.getCron().installTick(configHash, "prefix-", List.of(
            new CronMessage<>("old-msg", clock.now().plusSeconds(5))
        ));
        
        var oldKey = CronMessage.key(configHash, "prefix-", clock.now().plusSeconds(5));
        assertTrue(queue.containsMessage(oldKey));
        
        // Install new set - should delete old ones
        queue.getCron().installTick(configHash, "prefix-", List.of(
            new CronMessage<>("new-msg", clock.now().plusSeconds(10))
        ));
        
        assertFalse(queue.containsMessage(oldKey));
        var newKey = CronMessage.key(configHash, "prefix-", clock.now().plusSeconds(10));
        assertTrue(queue.containsMessage(newKey));
    }
    
    @Test
    public void install_returnsAutoCloseable() throws Exception {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        try (var handle = queue.getCron().install(
            configHash,
            "prefix-",
            Duration.ofSeconds(1),
            (Instant now) -> List.of(new CronMessage<>("msg", now.plusSeconds(5)))
        )) {
            assertNotNull(handle);
        }
    }
    
    @Test
    public void installPeriodicTick_returnsAutoCloseable() throws Exception {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        try (var handle = queue.getCron().installPeriodicTick(
            "key",
            Duration.ofSeconds(10),
            (Instant timestamp) -> "tick-at-" + timestamp.getEpochSecond()
        )) {
            assertNotNull(handle);
        }
    }
    
    @Test
    public void installDailySchedule_returnsAutoCloseable() throws Exception {
        var clock = Clock.test(Instant.parse("2024-01-01T10:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var schedule = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ZERO,
            Duration.ofSeconds(1)
        );
        
        try (var handle = queue.getCron().installDailySchedule(
            "prefix-",
            schedule,
            (Instant timestamp) -> new CronMessage<>("msg-" + timestamp, timestamp)
        )) {
            assertNotNull(handle);
        }
    }
    
    @Test
    public void configHash_differentConfigsHaveDifferentHashes() {
        var hash1 = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var hash2 = ConfigHash.fromPeriodicTick(Duration.ofHours(2));
        
        assertNotEquals(hash1.value(), hash2.value());
    }
    
    @Test
    public void configHash_sameConfigsHaveSameHashes() {
        var hash1 = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var hash2 = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        assertEquals(hash1.value(), hash2.value());
    }
    
    @Test
    public void dailyCronSchedule_getNextTimes_calculatesCorrectly() {
        var schedule = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00"), LocalTime.parse("18:00:00")),
            Duration.ZERO,
            Duration.ofSeconds(1)
        );
        
        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);
        
        assertFalse(nextTimes.isEmpty());
        assertEquals(Instant.parse("2024-01-01T12:00:00Z"), nextTimes.getFirst());
    }
    
    @Test
    public void dailyCronSchedule_withScheduleInAdvance_schedulesMultipleDays() {
        var schedule = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(2),
            Duration.ofSeconds(1)
        );
        
        var now = Instant.parse("2024-01-01T10:00:00Z");
        var nextTimes = schedule.getNextTimes(now);
        
        // Should have messages for today, tomorrow, and day after
        assertTrue(nextTimes.size() >= 3);
        assertTrue(nextTimes.contains(Instant.parse("2024-01-01T12:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-02T12:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-03T12:00:00Z")));
    }
    
    @Test
    public void cronMessage_keyGeneration_isConsistent() {
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var timestamp = Instant.parse("2024-01-01T12:00:00Z");
        
        var key1 = CronMessage.key(configHash, "prefix-", timestamp);
        var key2 = CronMessage.key(configHash, "prefix-", timestamp);
        
        assertEquals(key1, key2);
        assertTrue(key1.startsWith("prefix-/"));
    }
    
    @Test
    public void cronMessage_toScheduled_createsCorrectMessage() {
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var cronMsg = new CronMessage<>("payload", Instant.parse("2024-01-01T12:00:00Z"));
        
        var scheduled = cronMsg.toScheduled(configHash, "prefix-", true);
        
        assertEquals("payload", scheduled.payload());
        assertEquals(Instant.parse("2024-01-01T12:00:00Z"), scheduled.scheduleAt());
        assertTrue(scheduled.canUpdate());
        assertTrue(scheduled.key().startsWith("prefix-/"));
    }
    
    // ========== Additional Kotlin Tests Converted to Java ==========
    
    @Test
    public void installTick_messagesWithMultipleKeys() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        var messages = List.of(
            new CronMessage<>("msg1", clock.now().plusSeconds(10)),
            new CronMessage<>("msg2", clock.now().plusSeconds(20)),
            new CronMessage<>("msg3", clock.now().plusSeconds(30))
        );
        
        queue.getCron().installTick(configHash, "test-", messages);
        
        // All messages should be in the queue
        var key1 = CronMessage.key(configHash, "test-", clock.now().plusSeconds(10));
        var key2 = CronMessage.key(configHash, "test-", clock.now().plusSeconds(20));
        var key3 = CronMessage.key(configHash, "test-", clock.now().plusSeconds(30));
        
        assertTrue(queue.containsMessage(key1));
        assertTrue(queue.containsMessage(key2));
        assertTrue(queue.containsMessage(key3));
    }
    
    @Test
    public void installTick_messagesBecomeAvailableAtScheduledTime() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        var messages = List.of(
            new CronMessage<>("msg1", clock.now().plusSeconds(10)),
            new CronMessage<>("msg2", clock.now().plusSeconds(20))
        );
        
        queue.getCron().installTick(configHash, "test-", messages);
        
        // Not available yet
        assertNull(queue.tryPoll());
        
        // First message available
        clock.advance(Duration.ofSeconds(15));
        var env1 = queue.tryPoll();
        assertNotNull(env1);
        assertEquals("msg1", env1.getPayload());
        env1.acknowledge();
        
        // Second message not yet available
        assertNull(queue.tryPoll());
        
        // Second message available
        clock.advance(Duration.ofSeconds(10));
        var env2 = queue.tryPoll();
        assertNotNull(env2);
        assertEquals("msg2", env2.getPayload());
        env2.acknowledge();
    }
    
    @Test
    public void uninstallTick_removesAllMessagesWithPrefix() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        var messages = List.of(
            new CronMessage<>("msg1", clock.now().plusSeconds(10)),
            new CronMessage<>("msg2", clock.now().plusSeconds(20))
        );
        
        queue.getCron().installTick(configHash, "test-", messages);
        
        // Verify messages exist
        var key1 = CronMessage.key(configHash, "test-", clock.now().plusSeconds(10));
        var key2 = CronMessage.key(configHash, "test-", clock.now().plusSeconds(20));
        assertTrue(queue.containsMessage(key1));
        assertTrue(queue.containsMessage(key2));
        
        // Uninstall
        queue.getCron().uninstallTick(configHash, "test-");
        
        // Messages should be gone
        assertFalse(queue.containsMessage(key1));
        assertFalse(queue.containsMessage(key2));
    }
    
    @Test
    public void uninstallTick_onlyRemovesMatchingPrefix() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        // Install messages with different prefixes
        queue.getCron().installTick(configHash, "prefix1-", List.of(
            new CronMessage<>("msg1", clock.now().plusSeconds(10))
        ));
        queue.getCron().installTick(configHash, "prefix2-", List.of(
            new CronMessage<>("msg2", clock.now().plusSeconds(10))
        ));
        
        var key1 = CronMessage.key(configHash, "prefix1-", clock.now().plusSeconds(10));
        var key2 = CronMessage.key(configHash, "prefix2-", clock.now().plusSeconds(10));
        
        // Uninstall only prefix1
        queue.getCron().uninstallTick(configHash, "prefix1-");
        
        // prefix1 gone, prefix2 remains
        assertFalse(queue.containsMessage(key1));
        assertTrue(queue.containsMessage(key2));
    }
    
    @Test
    public void installTick_withEmptyList_deletesOldMessages() throws InterruptedException, SQLException {
        var clock = Clock.test(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        // Install messages
        queue.getCron().installTick(configHash, "cron-", List.of(
            new CronMessage<>("msg", clock.now().plusSeconds(10))
        ));
        
        var key = CronMessage.key(configHash, "cron-", clock.now().plusSeconds(10));
        assertTrue(queue.containsMessage(key));
        
        // Install empty list
        queue.getCron().installTick(configHash, "cron-", List.of());
        
        // Old message should be deleted
        assertFalse(queue.containsMessage(key));
    }
    
    @Test
    public void dailyCronSchedule_getNextTimes_skipsCurrentHour() {
        var schedule = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00"), LocalTime.parse("18:00:00")),
            Duration.ZERO,
            Duration.ofSeconds(1)
        );
        
        var now = Instant.parse("2024-01-01T12:00:00Z"); // Exact 12:00
        var nextTimes = schedule.getNextTimes(now);
        
        assertEquals(1, nextTimes.size());
        // Should skip 12:00 (current time) and go to 18:00
        assertEquals(Instant.parse("2024-01-01T18:00:00Z"), nextTimes.getFirst());
    }
    
    @Test
    public void dailyCronSchedule_getNextTimes_schedulesNextDay() {
        var schedule = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00"), LocalTime.parse("18:00:00")),
            Duration.ZERO,
            Duration.ofSeconds(1)
        );
        
        var now = Instant.parse("2024-01-01T20:00:00Z");
        var nextTimes = schedule.getNextTimes(now);
        
        assertEquals(1, nextTimes.size());
        assertEquals(Instant.parse("2024-01-02T12:00:00Z"), nextTimes.getFirst());
    }
    
    @Test
    public void dailyCronSchedule_withMultipleHoursPerDay() {
        var schedule = DailyCronSchedule.create(
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
        
        // Should schedule remaining today + all tomorrow
        assertTrue(nextTimes.size() >= 4);
        assertTrue(nextTimes.contains(Instant.parse("2024-01-01T12:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-01T18:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-02T09:00:00Z")));
        assertTrue(nextTimes.contains(Instant.parse("2024-01-02T12:00:00Z")));
    }
    
    @Test
    public void configHash_fromDailyCron_isDeterministic() {
        var schedule = DailyCronSchedule.create(
            ZoneId.of("Europe/Amsterdam"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );
        
        var hash1 = ConfigHash.fromDailyCron(schedule);
        var hash2 = ConfigHash.fromDailyCron(schedule);
        
        assertEquals(hash1, hash2);
    }
    
    @Test
    public void configHash_fromDailyCron_changesWithDifferentTimezone() {
        var schedule1 = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );
        var schedule2 = DailyCronSchedule.create(
            ZoneId.of("America/New_York"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );
        
        var hash1 = ConfigHash.fromDailyCron(schedule1);
        var hash2 = ConfigHash.fromDailyCron(schedule2);
        
        assertNotEquals(hash1, hash2);
    }
    
    @Test
    public void configHash_fromDailyCron_changesWithDifferentHours() {
        var schedule1 = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("12:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );
        var schedule2 = DailyCronSchedule.create(
            ZoneId.of("UTC"),
            List.of(LocalTime.parse("18:00:00")),
            Duration.ofDays(1),
            Duration.ofSeconds(1)
        );
        
        var hash1 = ConfigHash.fromDailyCron(schedule1);
        var hash2 = ConfigHash.fromDailyCron(schedule2);
        
        assertNotEquals(hash1, hash2);
    }
    
    @Test
    public void cronMessage_withScheduleAtActual_usesDifferentExecutionTime() {
        var configHash = ConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var nominal = Instant.parse("2024-01-01T12:00:00Z");
        var actual = Instant.parse("2024-01-01T12:05:00Z");
        var cronMsg = new CronMessage<>("payload", nominal, actual);
        
        var scheduled = cronMsg.toScheduled(configHash, "prefix-", false);
        
        // Key uses nominal time
        var expectedKey = CronMessage.key(configHash, "prefix-", nominal);
        assertEquals(expectedKey, scheduled.key());
        
        // Schedule uses actual time
        assertEquals(actual, scheduled.scheduleAt());
    }
}
