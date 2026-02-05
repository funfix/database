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
 * All tests use MutableClock for fast, deterministic execution.
 */
public class CronServiceTest {

    @Test
    public void installTick_createsMessagesInQueue() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var messages = List.of(
            new CronMessage<>("msg1", clock.now().plusSeconds(10)),
            new CronMessage<>("msg2", clock.now().plusSeconds(20))
        );
        
        queue.getCron().installTick(configHash, "cron-prefix-", messages);
        
        // Messages should be available when time advances
        clock.advance(Duration.ofSeconds(15));
        var envelope1 = queue.tryPoll();
        assertNotNull(envelope1);
        assertEquals("msg1", envelope1.payload());
        envelope1.acknowledge();
        
        clock.advance(Duration.ofSeconds(10));
        var envelope2 = queue.tryPoll();
        assertNotNull(envelope2);
        assertEquals("msg2", envelope2.payload());
        envelope2.acknowledge();
    }
    
    @Test
    public void uninstallTick_removesMessagesFromQueue() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
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
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
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
    public void installTick_replacesPreviousConfigurationWithSamePrefix() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );

        var configHashA = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var configHashB = CronConfigHash.fromPeriodicTick(Duration.ofHours(2));

        queue.getCron().installTick(configHashA, "prefix-", List.of(
            new CronMessage<>("msg-a", clock.now().plusSeconds(5))
        ));

        var keyA = CronMessage.key(configHashA, "prefix-", clock.now().plusSeconds(5));
        assertTrue(queue.containsMessage(keyA));

        queue.getCron().installTick(configHashB, "prefix-", List.of(
            new CronMessage<>("msg-b", clock.now().plusSeconds(10))
        ));

        var keyB = CronMessage.key(configHashB, "prefix-", clock.now().plusSeconds(10));
        assertTrue(queue.containsMessage(keyB));
        assertFalse(queue.containsMessage(keyA));

        clock.advance(Duration.ofSeconds(10));
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        assertEquals("msg-b", envelope.payload());
        envelope.acknowledge();
        assertNull(queue.tryPoll());
    }
    
    @Test
    public void install_returnsAutoCloseable() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
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
    public void install_rejectsNonPositiveScheduleInterval() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));

        //noinspection resource
        assertThrows(IllegalArgumentException.class, () -> queue.getCron().install(
            configHash,
            "prefix-",
            Duration.ZERO,
            (Instant now) -> List.of(new CronMessage<>("msg", now.plusSeconds(5)))
        ));
    }
    
    @Test
    public void installPeriodicTick_returnsAutoCloseable() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
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
    public void installPeriodicTick_rejectsNonPositivePeriod() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );

        //noinspection resource
        assertThrows(IllegalArgumentException.class, () -> queue.getCron().installPeriodicTick(
            "key",
            Duration.ZERO,
            (Instant timestamp) -> "tick-at-" + timestamp.getEpochSecond()
        ));
    }
    
    @Test
    public void installDailySchedule_returnsAutoCloseable() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var schedule = CronDailySchedule.create(
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
        var hash1 = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var hash2 = CronConfigHash.fromPeriodicTick(Duration.ofHours(2));
        
        assertNotEquals(hash1.value(), hash2.value());
    }
    
    @Test
    public void configHash_sameConfigsHaveSameHashes() {
        var hash1 = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var hash2 = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
        assertEquals(hash1.value(), hash2.value());
    }
    
    @Test
    public void cronMessage_keyGeneration_isConsistent() {
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var timestamp = Instant.parse("2024-01-01T12:00:00Z");
        
        var key1 = CronMessage.key(configHash, "prefix-", timestamp);
        var key2 = CronMessage.key(configHash, "prefix-", timestamp);
        
        assertEquals(key1, key2);
        assertTrue(key1.startsWith("prefix-/"));
    }

    @Test
    public void cronMessage_keyIncludesSubSecondPrecision() {
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var timestampA = Instant.parse("2024-01-01T12:00:00.100Z");
        var timestampB = Instant.parse("2024-01-01T12:00:00.900Z");

        var keyA = CronMessage.key(configHash, "prefix-", timestampA);
        var keyB = CronMessage.key(configHash, "prefix-", timestampB);

        assertNotEquals(keyA, keyB);
    }
    
    @Test
    public void cronMessage_toScheduled_createsCorrectMessage() {
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
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
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
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
    public void installTick_allowsMultipleMessagesInSameSecond() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var timeA = Instant.parse("2024-01-01T00:00:00.100Z");
        var timeB = Instant.parse("2024-01-01T00:00:00.900Z");

        queue.getCron().installTick(configHash, "test-", List.of(
            new CronMessage<>("msg1", timeA),
            new CronMessage<>("msg2", timeB)
        ));

        var keyA = CronMessage.key(configHash, "test-", timeA);
        var keyB = CronMessage.key(configHash, "test-", timeB);

        assertTrue(queue.containsMessage(keyA));
        assertTrue(queue.containsMessage(keyB));
        assertNotEquals(keyA, keyB);
    }
    
    @Test
    public void installTick_messagesBecomeAvailableAtScheduledTime() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
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
        assertEquals("msg1", env1.payload());
        env1.acknowledge();
        
        // Second message not yet available
        assertNull(queue.tryPoll());
        
        // Second message available
        clock.advance(Duration.ofSeconds(10));
        var env2 = queue.tryPoll();
        assertNotNull(env2);
        assertEquals("msg2", env2.payload());
        env2.acknowledge();
    }
    
    @Test
    public void uninstallTick_removesAllMessagesWithPrefix() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
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
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
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
    public void uninstallTick_onlyRemovesMatchingConfigHash() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHashA = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        var configHashB = CronConfigHash.fromPeriodicTick(Duration.ofHours(2));

        queue.getCron().installTick(configHashA, "prefix-", List.of(
            new CronMessage<>("msg-a", clock.now().plusSeconds(10))
        ));
        queue.getCron().installTick(configHashB, "prefix-", List.of(
            new CronMessage<>("msg-b", clock.now().plusSeconds(10))
        ));

        var keyA = CronMessage.key(configHashA, "prefix-", clock.now().plusSeconds(10));
        var keyB = CronMessage.key(configHashB, "prefix-", clock.now().plusSeconds(10));

        queue.getCron().uninstallTick(configHashA, "prefix-");

        assertFalse(queue.containsMessage(keyA));
        assertTrue(queue.containsMessage(keyB));
    }
    
    @Test
    public void installTick_withEmptyList_deletesOldMessages() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
        
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
    public void installTick_doesNotDropRedeliveryMessages() throws InterruptedException, SQLException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var timeConfig = DelayedQueueTimeConfig.create(Duration.ofSeconds(5), Duration.ofMillis(100));
        var queue = DelayedQueueInMemory.<String>create(timeConfig, "test-source", clock);
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));

        queue.getCron().installTick(configHash, "cron-", List.of(
            new CronMessage<>("msg", clock.now())
        ));

        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        assertEquals(DeliveryType.FIRST_DELIVERY, envelope.deliveryType());

        queue.getCron().installTick(configHash, "cron-", List.of());

        clock.advance(Duration.ofSeconds(6));
        var redelivery = queue.tryPoll();
        assertNotNull(redelivery);
        assertEquals(DeliveryType.REDELIVERY, redelivery.deliveryType());
    }

    @Test
    public void install_loopDoesNotDropRedeliveryMessages() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var timeConfig = DelayedQueueTimeConfig.create(Duration.ofMillis(50), Duration.ofMillis(10));
        var queue = DelayedQueueInMemory.<String>create(timeConfig, "test-source", clock);
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));

        try (var ignored = queue.getCron().install(
            configHash,
            "loop-",
            Duration.ofMillis(10),
            (Instant now) -> List.of(new CronMessage<>("msg", now))
        )) {
            AckEnvelope<String> envelope = null;
            var waitUntil = System.nanoTime() + Duration.ofSeconds(1).toNanos();
            while (envelope == null && System.nanoTime() < waitUntil) {
                envelope = queue.tryPoll();
                if (envelope == null) {
                    //noinspection BusyWait
                    Thread.sleep(5);
                }
            }

            assertNotNull(envelope);
            assertEquals(DeliveryType.FIRST_DELIVERY, envelope.deliveryType());

            clock.advance(Duration.ofMillis(60));
            Thread.sleep(30);

            var redelivery = queue.tryPoll();
            assertNotNull(redelivery);
            assertEquals(DeliveryType.REDELIVERY, redelivery.deliveryType());
        }
    }
    
    @Test
    public void cronMessage_withScheduleAtActual_usesDifferentExecutionTime() {
        var configHash = CronConfigHash.fromPeriodicTick(Duration.ofHours(1));
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
