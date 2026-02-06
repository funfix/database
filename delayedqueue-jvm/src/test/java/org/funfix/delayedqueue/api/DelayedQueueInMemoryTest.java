package org.funfix.delayedqueue.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.funfix.delayedqueue.jvm.*;
import org.junit.jupiter.api.Test;

/**
 * Java API tests for DelayedQueueInMemory.
 * Tests the complete public API without accessing any internals.
 */
public class DelayedQueueInMemoryTest {
    
    // ========== Basic Operations ==========
    
    @Test
    public void offerOrUpdate_createsNewMessage() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now().plusSeconds(10);
        
        var result = queue.offerOrUpdate("key1", "payload1", scheduleAt);

        assertInstanceOf(OfferOutcome.Created.class, result);
    }
    
    @Test
    public void offerOrUpdate_updatesExistingMessage() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now().plusSeconds(10);
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var result = queue.offerOrUpdate("key1", "payload2", scheduleAt.plusSeconds(5));

        assertInstanceOf(OfferOutcome.Updated.class, result);
    }
    
    @Test
    public void offerIfNotExists_createsNewMessage() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now().plusSeconds(10);
        
        var result = queue.offerIfNotExists("key1", "payload1", scheduleAt);

        assertInstanceOf(OfferOutcome.Created.class, result);
    }
    
    @Test
    public void offerIfNotExists_ignoresExistingMessage() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now().plusSeconds(10);
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var result = queue.offerIfNotExists("key1", "payload2", scheduleAt.plusSeconds(5));

        assertInstanceOf(OfferOutcome.Ignored.class, result);
    }
    
    // ========== Polling Operations ==========
    
    @Test
    public void tryPoll_returnsNullWhenQueueIsEmpty() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var result = queue.tryPoll();
        
        assertNull(result);
    }
    
    @Test
    public void tryPoll_returnsNullWhenMessageNotReadyYet() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var future = clock.now().plusSeconds(10);
        
        queue.offerOrUpdate("key1", "payload1", future);
        var result = queue.tryPoll();
        
        assertNull(result);
    }
    
    @Test
    public void tryPoll_returnsMessageWhenReady() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var envelope = queue.tryPoll();
        
        assertNotNull(envelope);
        assertEquals("payload1", envelope.payload());
        assertEquals(DeliveryType.FIRST_DELIVERY, envelope.deliveryType());
    }
    
    @Test
    public void tryPoll_respectsFIFOOrdering() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        queue.offerOrUpdate("key2", "payload2", scheduleAt);
        queue.offerOrUpdate("key3", "payload3", scheduleAt);
        
        var msg1 = queue.tryPoll();
        var msg2 = queue.tryPoll();
        var msg3 = queue.tryPoll();
        
        assertNotNull(msg1);
        assertNotNull(msg2);
        assertNotNull(msg3);
        assertEquals("payload1", msg1.payload());
        assertEquals("payload2", msg2.payload());
        assertEquals("payload3", msg3.payload());
    }
    
    @Test
    public void tryPoll_marksMessageForRedeliveryAfterTimeout() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var timeConfig = DelayedQueueTimeConfig.create(Duration.ofSeconds(5), Duration.ofMillis(100));
        var queue = DelayedQueueInMemory.<String>create(timeConfig, "test-source", clock);
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var envelope1 = queue.tryPoll();
        assertNotNull(envelope1);
        assertEquals(DeliveryType.FIRST_DELIVERY, envelope1.deliveryType());
        
        // Don't acknowledge, advance time past timeout
        clock.advance(Duration.ofSeconds(6));
        
        var envelope2 = queue.tryPoll();
        assertNotNull(envelope2);
        assertEquals("payload1", envelope2.payload());
        assertEquals(DeliveryType.REDELIVERY, envelope2.deliveryType());
    }
    
    @Test
    public void tryPollMany_returnsEmptyListWhenQueueIsEmpty() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var envelope = queue.tryPollMany(10);
        
        assertNotNull(envelope);
        assertTrue(envelope.payload().isEmpty());
    }
    
    @Test
    public void tryPollMany_returnsAvailableMessagesUpToLimit() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        queue.offerOrUpdate("key2", "payload2", scheduleAt);
        queue.offerOrUpdate("key3", "payload3", scheduleAt);
        queue.offerOrUpdate("key4", "payload4", scheduleAt);
        
        var envelope = queue.tryPollMany(2);
        
        assertNotNull(envelope);
        assertEquals(2, envelope.payload().size());
    }
    
    @Test
    public void tryPollMany_doesNotReturnFutureMessages() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var now = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", now);
        queue.offerOrUpdate("key2", "payload2", now.plusSeconds(10));
        queue.offerOrUpdate("key3", "payload3", now.plusSeconds(20));
        
        var envelope = queue.tryPollMany(10);
        
        assertNotNull(envelope);
        assertEquals(1, envelope.payload().size());
        assertEquals("payload1", envelope.payload().getFirst());
    }

    @Test
    public void tryPollMany_marksBatchAsRedeliveryWhenAnyMessageIsRedelivered() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var timeConfig = DelayedQueueTimeConfig.create(Duration.ofSeconds(5), Duration.ofMillis(100));
        var queue = DelayedQueueInMemory.<String>create(timeConfig, "test-source", clock);
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
    
    // ========== Acknowledgment ==========
    
    @Test
    public void acknowledge_removesMessageFromQueue() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        
        envelope.acknowledge();
        
        // Message should not be available for redelivery
        clock.advance(Duration.ofMinutes(10));
        assertNull(queue.tryPoll());
    }
    
    @Test
    public void acknowledge_isIdempotent() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        
        envelope.acknowledge();
        envelope.acknowledge(); // Second call should be safe
        
        clock.advance(Duration.ofMinutes(10));
        assertNull(queue.tryPoll());
    }
    
    @Test
    public void acknowledge_doesNotRemoveIfMessageWasUpdated() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        
        // Update the message before acknowledging
        queue.offerOrUpdate("key1", "payload2", scheduleAt);
        envelope.acknowledge();
        
        // The updated message should still be available
        var envelope2 = queue.tryPoll();
        assertNotNull(envelope2);
        assertEquals("payload2", envelope2.payload());
    }
    
    @Test
    public void batchAcknowledge_removesAllMessages() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        queue.offerOrUpdate("key2", "payload2", scheduleAt);
        queue.offerOrUpdate("key3", "payload3", scheduleAt);
        
        var envelope = queue.tryPollMany(10);
        assertNotNull(envelope);
        assertEquals(3, envelope.payload().size());
        
        envelope.acknowledge();
        
        clock.advance(Duration.ofMinutes(10));
        assertNull(queue.tryPoll());
    }
    
    // ========== Read Operation ==========
    
    @Test
    public void read_returnsMessageWithoutRemovingIt() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var msg = queue.read("key1");
        
        assertNotNull(msg);
        assertEquals("payload1", msg.payload());
        
        // Message should still be available
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        assertEquals("payload1", envelope.payload());
    }
    
    @Test
    public void read_returnsNullForNonExistentKey() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        var msg = queue.read("nonexistent");
        
        assertNull(msg);
    }
    
    // ========== Concurrency ==========
    
    @Test
    public void concurrentOperations_areThreadSafe() throws InterruptedException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        try (var executor = Executors.newFixedThreadPool(4)) {
            var latch = new CountDownLatch(1000);
            var scheduleAt = clock.now();

            // Offer 1000 messages concurrently
            for (int i = 0; i < 1000; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        queue.offerOrUpdate("key" + index, "payload" + index, scheduleAt);
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));

            // Poll all messages
            var polled = new ArrayList<String>();
            while (true) {
                try {
                    var envelope = queue.tryPoll();
                    if (envelope == null) break;
                    polled.add(envelope.payload());
                    envelope.acknowledge();
                } catch (Exception e) {
                    break;
                }
            }

            assertEquals(1000, polled.size());
            assertEquals(1000, polled.stream().distinct().count()); // All unique

            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
    
    @Test
    public void poll_blocksUntilMessageIsAvailable() throws InterruptedException {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now();
        var polled = new AtomicInteger(0);
        var latch = new CountDownLatch(1);
        
        // Start a thread that will poll (blocking)
        var thread = new Thread(() -> {
            try {
                var envelope = queue.poll();
                if (envelope.payload().equals("payload1")) {
                    polled.incrementAndGet();
                }
                envelope.acknowledge();
            } catch (InterruptedException e) {
                // Expected when we interrupt
            } catch (Exception e) {
                // Ignore
            } finally {
                latch.countDown();
            }
        });
        thread.start();
        
        // Give thread time to start waiting
        Thread.sleep(100);
        
        // Offer a message
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        
        // Wait for poll to complete
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, polled.get());
        
        thread.join(1000);
    }
    
    // ========== Future Message Timing Tests ==========
    
    @Test
    public void futureMessage_becomesAvailableAtExactScheduledTime() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now().plusSeconds(100);
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        
        // 1 second before
        clock.setTime(scheduleAt.minusSeconds(1));
        assertNull(queue.tryPoll());
        
        // Exact time
        clock.setTime(scheduleAt);
        var envelope = queue.tryPoll();
        assertNotNull(envelope);
        assertEquals("payload1", envelope.payload());
    }
    
    @Test
    public void multipleFutureMessages_becomeAvailableInOrder() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var base = clock.now();
        
        queue.offerOrUpdate("key1", "payload1", base.plusSeconds(10));
        queue.offerOrUpdate("key2", "payload2", base.plusSeconds(20));
        queue.offerOrUpdate("key3", "payload3", base.plusSeconds(30));
        
        clock.setTime(base.plusSeconds(15));
        assertEquals("payload1", Objects.requireNonNull(queue.tryPoll()).payload());
        assertNull(queue.tryPoll());
        
        clock.setTime(base.plusSeconds(25));
        assertEquals("payload2", Objects.requireNonNull(queue.tryPoll()).payload());
        assertNull(queue.tryPoll());
        
        clock.setTime(base.plusSeconds(35));
        assertEquals("payload3", Objects.requireNonNull(queue.tryPoll()).payload());
        assertNull(queue.tryPoll());
    }
    
    // ========== Additional Contract Tests ==========
    
    @Test
    public void offerOrUpdate_ignoresIdenticalMessage() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        var scheduleAt = clock.now().plusSeconds(10);
        
        queue.offerOrUpdate("key1", "payload1", scheduleAt);
        var result = queue.offerOrUpdate("key1", "payload1", scheduleAt);
        
        assertInstanceOf(OfferOutcome.Ignored.class, result);
    }
    
    @Test
    public void dropMessage_removesMessage() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        queue.offerOrUpdate("key1", "payload1", clock.now().plusSeconds(10));
        
        assertTrue(queue.dropMessage("key1"));
        assertFalse(queue.containsMessage("key1"));
    }
    
    @Test
    public void dropMessage_returnsFalseForNonExistentKey() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        assertFalse(queue.dropMessage("non-existent"));
    }
    
    @Test
    public void offerBatch_createsMultipleMessages() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
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
    public void offerBatch_handlesUpdatesCorrectly() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        queue.offerOrUpdate("key1", "original", clock.now().plusSeconds(10));
        
        var messages = List.of(
            new BatchedMessage<>(1, new ScheduledMessage<>("key1", "updated", clock.now().plusSeconds(20), true)),
            new BatchedMessage<>(2, new ScheduledMessage<>("key2", "new", clock.now().plusSeconds(30)))
        );
        
        var results = queue.offerBatch(messages);
        
        assertEquals(2, results.size());
        assertInstanceOf(OfferOutcome.Updated.class, results.get(0).outcome());
        assertInstanceOf(OfferOutcome.Created.class, results.get(1).outcome());
    }
    
    @Test
    public void dropAllMessages_removesAllMessages() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        queue.offerOrUpdate("key1", "payload1", clock.now().plusSeconds(10));
        queue.offerOrUpdate("key2", "payload2", clock.now().plusSeconds(20));
        
        var count = queue.dropAllMessages("Yes, please, I know what I'm doing!");
        
        assertEquals(2, count);
        assertFalse(queue.containsMessage("key1"));
        assertFalse(queue.containsMessage("key2"));
    }
    
    @Test
    public void dropAllMessages_requiresConfirmation() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
        
        assertThrows(IllegalArgumentException.class, () ->
            queue.dropAllMessages("wrong confirmation")
        );
    }
    
    @Test
    public void pollAck_onlyDeletesIfNoUpdateHappenedInBetween() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
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
    public void readAck_onlyDeletesIfNoUpdateHappenedInBetween() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
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
    public void tryPollMany_withBatchSizeSmallerThanPagination() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
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
    public void tryPollMany_withBatchSizeEqualToPagination() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
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
    public void tryPollMany_withBatchSizeLargerThanPagination() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
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
    public void tryPollMany_withMaxSizeLessThanOrEqualToZero_returnsEmptyBatch() {
        var clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"));
        var queue = DelayedQueueInMemory.<String>create(
            DelayedQueueTimeConfig.create(Duration.ofSeconds(30), Duration.ofMillis(100)),
            "test-source",
            clock
        );
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
