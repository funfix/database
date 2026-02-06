package org.funfix.delayedqueue.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.funfix.delayedqueue.jvm.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Java API tests for DelayedQueueJDBC backed by SQLite.
 * Mirrors the existing HSQLDB tests to ensure feature parity.
 */
public class DelayedQueueSqliteTest {
    
    private DelayedQueueJDBC<String> queue;
    
    @AfterEach
    public void cleanup() {
        if (queue != null) {
            try {
                queue.dropAllMessages("Yes, please, I know what I'm doing!");
                queue.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }
    
    private static String uniqueSqliteUrl() {
        try {
            var tmpFile = java.io.File.createTempFile("testdb_", ".sqlite");
            tmpFile.deleteOnExit();
            return "jdbc:sqlite:" + tmpFile.getAbsolutePath();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DelayedQueueJDBC<String> createQueue() throws Exception {
        var url = uniqueSqliteUrl();
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.Sqlite
        );
        
        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "sqlite-test-queue");
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }

    private DelayedQueueJDBC<String> createQueueWithClock(MutableClock clock) throws Exception {
        var url = uniqueSqliteUrl();
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.Sqlite
        );
        
        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "sqlite-test-queue");
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }
    
    private DelayedQueueJDBC<String> createQueueWithClock(MutableClock clock, DelayedQueueTimeConfig timeConfig) throws Exception {
        var url = uniqueSqliteUrl();
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.Sqlite
        );
        
        var queueConfig = new DelayedQueueJDBCConfig(dbConfig, "delayed_queue_test", timeConfig, "sqlite-test-queue");
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }
    
    // ========== Contract Tests ==========
    
    @Test
    public void offerIfNotExists_createsNewMessage() throws Exception {
        queue = createQueue();
        var now = Instant.now();
        
        var result = queue.offerIfNotExists("key1", "payload1", now.plusSeconds(10));
        
        assertInstanceOf(OfferOutcome.Created.class, result);
        assertTrue(queue.containsMessage("key1"));
    }
    
    @Test
    public void offerIfNotExists_ignoresDuplicateKey() throws Exception {
        queue = createQueue();
        var now = Instant.now();
        
        queue.offerIfNotExists("key1", "payload1", now.plusSeconds(10));
        var result = queue.offerIfNotExists("key1", "payload2", now.plusSeconds(20));
        
        assertInstanceOf(OfferOutcome.Ignored.class, result);
    }
    
    @Test
    public void offerOrUpdate_createsNewMessage() throws Exception {
        queue = createQueue();
        var now = Instant.now();
        
        var result = queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10));
        
        assertInstanceOf(OfferOutcome.Created.class, result);
        assertTrue(queue.containsMessage("key1"));
    }
    
    @Test
    public void offerOrUpdate_updatesExistingMessage() throws Exception {
        queue = createQueue();
        var now = Instant.now();
        
        queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10));
        var result = queue.offerOrUpdate("key1", "payload2", now.plusSeconds(20));
        
        assertInstanceOf(OfferOutcome.Updated.class, result);
    }
    
    @Test
    public void offerOrUpdate_ignoresIdenticalMessage() throws Exception {
        queue = createQueue();
        var now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        
        queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10));
        var result = queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10));
        
        assertInstanceOf(OfferOutcome.Ignored.class, result);
    }
    
    @Test
    public void tryPoll_returnsNullWhenNoMessagesAvailable() throws Exception {
        queue = createQueue();
        
        var result = queue.tryPoll();
        
        assertNull(result);
    }
    
    @Test
    public void tryPoll_returnsMessageWhenAvailable() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().minusSeconds(10));
        var result = queue.tryPoll();
        
        assertNotNull(result);
        assertEquals("payload1", result.payload());
        assertEquals("key1", result.messageId().value());
    }
    
    @Test
    public void tryPoll_doesNotReturnFutureMessages() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", Instant.now().plusSeconds(60));
        var result = queue.tryPoll();
        
        assertNull(result);
    }
    
    @Test
    public void acknowledge_removesMessageFromQueue() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().minusSeconds(10));
        var message = queue.tryPoll();
        assertNotNull(message);
        
        message.acknowledge();
        
        assertFalse(queue.containsMessage("key1"));
    }
    
    @Test
    public void acknowledge_isIdempotent() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().minusSeconds(10));
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        
        envelope.acknowledge();
        envelope.acknowledge(); // Second call should be safe
        
        assertFalse(queue.containsMessage("key1"));
    }
    
    @Test
    public void acknowledge_doesNotRemoveIfMessageWasUpdated() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().minusSeconds(10));
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        
        // Update the message before acknowledging
        queue.offerOrUpdate("key1", "payload2", clock.now().minusSeconds(10));
        envelope.acknowledge();
        
        // The updated message should still be available
        var envelope2 = queue.tryPoll();
        assertNotNull(envelope2);
        assertEquals("payload2", envelope2.payload());
    }
    
    @Test
    public void tryPollMany_returnsEmptyListWhenNoMessages() throws Exception {
        queue = createQueue();
        
        var result = queue.tryPollMany(10);
        
        assertTrue(result.payload().isEmpty());
    }
    
    @Test
    public void tryPollMany_returnsAvailableMessages() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().minusSeconds(10));
        queue.offerOrUpdate("key2", "payload2", clock.now().minusSeconds(5));
        
        var result = queue.tryPollMany(10);
        
        assertEquals(2, result.payload().size());
        assertTrue(result.payload().contains("payload1"));
        assertTrue(result.payload().contains("payload2"));
    }
    
    @Test
    public void tryPollMany_respectsBatchSizeLimit() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        for (int i = 1; i <= 10; i++) {
            queue.offerOrUpdate("key" + i, "payload" + i, clock.now().minusSeconds(10));
        }
        
        var result = queue.tryPollMany(5);
        
        assertEquals(5, result.payload().size());
    }
    
    @Test
    public void tryPollMany_marksBatchAsRedeliveryWhenAnyMessageIsRedelivered() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var timeConfig = DelayedQueueTimeConfig.create(Duration.ofSeconds(5), Duration.ofMillis(100));
        queue = createQueueWithClock(clock, timeConfig);
        var scheduleAt = clock.now();

        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        queue.offerOrUpdate("key2", "payload2", scheduleAt);

        var first = queue.tryPoll();
        assertNotNull(first);
        assertEquals(DeliveryType.FIRST_DELIVERY, first.deliveryType());

        // Don't acknowledge, advance past timeout to trigger redelivery
        clock.advance(Duration.ofSeconds(6));

        var batch = queue.tryPollMany(10);

        assertNotNull(batch);
        assertEquals(2, batch.payload().size());
        assertEquals(DeliveryType.REDELIVERY, batch.deliveryType());
    }
    
    @Test
    public void read_retrievesMessageWithoutLocking() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().plusSeconds(10));
        var result = queue.read("key1");
        
        assertNotNull(result);
        assertEquals("payload1", result.payload());
        assertTrue(queue.containsMessage("key1"));
    }
    
    @Test
    public void dropMessage_removesMessage() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().plusSeconds(10));
        
        assertTrue(queue.dropMessage("key1"));
        assertFalse(queue.containsMessage("key1"));
    }
    
    @Test
    public void dropMessage_returnsFalseForNonExistentKey() throws Exception {
        queue = createQueue();
        
        assertFalse(queue.dropMessage("non-existent"));
    }
    
    @Test
    public void offerBatch_createsMultipleMessages() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        var messages = List.of(
            new BatchedMessage<>(1, new ScheduledMessage<>("key1", "payload1", clock.now().plusSeconds(10))),
            new BatchedMessage<>(2, new ScheduledMessage<>("key2", "payload2", clock.now().plusSeconds(20)))
        );
        
        var results = queue.offerBatch(messages);
        
        assertEquals(2, results.size());
        assertInstanceOf(OfferOutcome.Created.class, results.get(0).outcome());
        assertInstanceOf(OfferOutcome.Created.class, results.get(1).outcome());
        assertTrue(queue.containsMessage("key1"));
        assertTrue(queue.containsMessage("key2"));
    }
    
    @Test
    public void offerBatch_handlesUpdatesCorrectly() throws Exception {
        queue = createQueue();
        var now = Instant.now();
        
        queue.offerOrUpdate("key1", "original", now.plusSeconds(10));
        
        var messages = List.of(
            new BatchedMessage<>(1, new ScheduledMessage<>("key1", "updated", now.plusSeconds(20), true)),
            new BatchedMessage<>(2, new ScheduledMessage<>("key2", "new", now.plusSeconds(30)))
        );
        
        var results = queue.offerBatch(messages);
        
        assertEquals(2, results.size());
        assertInstanceOf(OfferOutcome.Updated.class, results.get(0).outcome());
        assertInstanceOf(OfferOutcome.Created.class, results.get(1).outcome());
    }
    
    @Test
    public void dropAllMessages_removesAllMessages() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().plusSeconds(10));
        queue.offerOrUpdate("key2", "payload2", clock.now().plusSeconds(20));
        
        var count = queue.dropAllMessages("Yes, please, I know what I'm doing!");
        
        assertEquals(2, count);
        assertFalse(queue.containsMessage("key1"));
        assertFalse(queue.containsMessage("key2"));
    }
    
    @Test
    public void dropAllMessages_requiresConfirmation() throws Exception {
        queue = createQueue();
        
        assertThrows(IllegalArgumentException.class, () -> 
            queue.dropAllMessages("wrong confirmation")
        );
    }
    
    @Test
    public void fifoOrdering_messagesPolledInScheduledOrder() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        var baseTime = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", baseTime.plusSeconds(3));
        queue.offerOrUpdate("key2", "payload2", baseTime.plusSeconds(1));
        queue.offerOrUpdate("key3", "payload3", baseTime.plusSeconds(2));
        
        clock.advance(Duration.ofSeconds(4));
        
        var msg1 = queue.tryPoll();
        var msg2 = queue.tryPoll();
        var msg3 = queue.tryPoll();
        
        assertEquals("payload2", Objects.requireNonNull(msg1).payload());
        assertEquals("payload3", Objects.requireNonNull(msg2).payload());
        assertEquals("payload1", Objects.requireNonNull(msg3).payload());
    }
    
    @Test
    public void poll_blocksUntilMessageAvailable() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        
        var pollThread = new Thread(() -> {
            try {
                var msg = queue.poll();
                assertEquals("payload1", msg.payload());
            } catch (InterruptedException e) {
                // Expected
            } catch (Exception e) {
                // Ignore
            }
        });
        
        pollThread.start();
        Thread.sleep(100);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().minusSeconds(1));
        
        pollThread.join(2000);
        assertFalse(pollThread.isAlive());
    }
    
    @Test
    public void redelivery_afterTimeout() throws Exception {
        var timeConfig = DelayedQueueTimeConfig.create(Duration.ofSeconds(5), Duration.ofMillis(10));
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock, timeConfig);
        
        queue.offerOrUpdate("key1", "payload1", clock.now().minusSeconds(10));
        
        var msg1 = queue.tryPoll();
        assertNotNull(msg1);
        assertEquals(DeliveryType.FIRST_DELIVERY, msg1.deliveryType());
        
        clock.advance(Duration.ofSeconds(6));
        
        var msg2 = queue.tryPoll();
        assertNotNull(msg2);
        assertEquals("payload1", msg2.payload());
        assertEquals(DeliveryType.REDELIVERY, msg2.deliveryType());
    }
    
    @Test
    public void pollAck_onlyDeletesIfNoUpdateHappenedInBetween() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        var now = clock.now();
        
        var offer1 = queue.offerOrUpdate("my-key", "value offered (1)", now.minusSeconds(1));
        assertInstanceOf(OfferOutcome.Created.class, offer1);
        
        var msg1 = queue.tryPoll();
        assertNotNull(msg1);
        assertEquals("value offered (1)", msg1.payload());
        
        var offer2 = queue.offerOrUpdate("my-key", "value offered (2)", now.minusSeconds(1));
        assertInstanceOf(OfferOutcome.Updated.class, offer2);
        
        var msg2 = queue.tryPoll();
        assertNotNull(msg2);
        assertEquals("value offered (2)", msg2.payload());
        
        msg1.acknowledge();
        assertTrue(queue.containsMessage("my-key"));
        
        msg2.acknowledge();
        assertFalse(queue.containsMessage("my-key"));
    }
    
    @Test
    public void readAck_onlyDeletesIfNoUpdateHappenedInBetween() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        var now = clock.now();
        
        queue.offerOrUpdate("my-key-1", "value offered (1.1)", now.minusSeconds(1));
        queue.offerOrUpdate("my-key-2", "value offered (2.1)", now.minusSeconds(1));
        queue.offerOrUpdate("my-key-3", "value offered (3.1)", now.minusSeconds(1));
        
        var msg1 = queue.read("my-key-1");
        var msg2 = queue.read("my-key-2");
        var msg3 = queue.read("my-key-3");
        var msg4 = queue.read("my-key-4");
        
        assertNotNull(msg1);
        assertNotNull(msg2);
        assertNotNull(msg3);
        assertNull(msg4);
        
        assertEquals("value offered (1.1)", msg1.payload());
        assertEquals("value offered (2.1)", msg2.payload());
        assertEquals("value offered (3.1)", msg3.payload());
        
        clock.advance(Duration.ofSeconds(1));
        
        queue.offerOrUpdate("my-key-2", "value offered (2.2)", now.minusSeconds(1));
        queue.offerOrUpdate("my-key-3", "value offered (3.1)", now);
        
        msg1.acknowledge();
        msg2.acknowledge();
        msg3.acknowledge();
        
        assertFalse(queue.containsMessage("my-key-1"));
        assertTrue(queue.containsMessage("my-key-2"));
        assertTrue(queue.containsMessage("my-key-3"));
        
        var remaining = queue.dropAllMessages("Yes, please, I know what I'm doing!");
        assertEquals(2, remaining);
    }
    
    @Test
    public void tryPollMany_withBatchSizeSmallerThanPagination() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        var now = clock.now();
        
        var messages = new ArrayList<BatchedMessage<Integer, String>>();
        for (int i = 0; i < 50; i++) {
            messages.add(new BatchedMessage<>(i, new ScheduledMessage<>(
                "key-" + i,
                "payload-" + i,
                now.minusSeconds(50 - i),
                false
            )));
        }
        queue.offerBatch(messages);
        
        var batch = queue.tryPollMany(50);
        assertEquals(50, batch.payload().size());
        
        for (int i = 0; i < 50; i++) {
            assertEquals("payload-" + i, batch.payload().get(i));
        }
        
        batch.acknowledge();
        
        var batch2 = queue.tryPollMany(10);
        assertTrue(batch2.payload().isEmpty());
    }
    
    @Test
    public void tryPollMany_withBatchSizeEqualToPagination() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        var now = clock.now();
        
        var messages = new ArrayList<BatchedMessage<Integer, String>>();
        for (int i = 0; i < 100; i++) {
            messages.add(new BatchedMessage<>(i, new ScheduledMessage<>(
                "key-" + i,
                "payload-" + i,
                now.minusSeconds(100 - i),
                false
            )));
        }
        queue.offerBatch(messages);
        
        var batch = queue.tryPollMany(100);
        assertEquals(100, batch.payload().size());
        
        for (int i = 0; i < 100; i++) {
            assertEquals("payload-" + i, batch.payload().get(i));
        }
        
        batch.acknowledge();
        
        var batch2 = queue.tryPollMany(3);
        assertTrue(batch2.payload().isEmpty());
    }
    
    @Test
    public void tryPollMany_withBatchSizeLargerThanPagination() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        var now = clock.now();
        
        var messages = new ArrayList<BatchedMessage<Integer, String>>();
        for (int i = 0; i < 250; i++) {
            messages.add(new BatchedMessage<>(i, new ScheduledMessage<>(
                "key-" + i,
                "payload-" + i,
                now.minusSeconds(250 - i),
                false
            )));
        }
        queue.offerBatch(messages);
        
        var batch = queue.tryPollMany(250);
        assertEquals(250, batch.payload().size());
        
        for (int i = 0; i < 250; i++) {
            assertEquals("payload-" + i, batch.payload().get(i));
        }
        
        batch.acknowledge();
        
        var batch2 = queue.tryPollMany(10);
        assertTrue(batch2.payload().isEmpty());
    }
    
    @Test
    public void tryPollMany_withMaxSizeLessThanOrEqualToZero_returnsEmptyBatch() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createQueueWithClock(clock);
        var now = clock.now();
        
        queue.offerOrUpdate("my-key-1", "value offered (1.1)", now.minusSeconds(1));
        queue.offerOrUpdate("my-key-2", "value offered (2.1)", now.minusSeconds(2));
        
        var batch0 = queue.tryPollMany(0);
        assertTrue(batch0.payload().isEmpty());
        batch0.acknowledge();
        
        var batch3 = queue.tryPollMany(3);
        assertEquals(2, batch3.payload().size());
        assertTrue(batch3.payload().contains("value offered (1.1)"));
        assertTrue(batch3.payload().contains("value offered (2.1)"));
    }
}
