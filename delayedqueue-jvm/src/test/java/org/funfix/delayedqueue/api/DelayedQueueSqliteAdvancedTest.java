package org.funfix.delayedqueue.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.funfix.delayedqueue.jvm.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Advanced JDBC-specific tests for SQLite including concurrency and multi-queue isolation.
 * Mirrors the existing HSQLDB advanced tests to ensure feature parity.
 */
public class DelayedQueueSqliteAdvancedTest {
    
    private final List<DelayedQueueJDBC<String>> queues = new java.util.ArrayList<>();
    
    @AfterEach
    public void cleanup() {
        for (var queue : queues) {
            try {
                queue.dropAllMessages("Yes, please, I know what I'm doing!");
                queue.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        queues.clear();
    }
    
    private static String uniqueSqliteUrl() {
        try {
            var tmpFile = java.io.File.createTempFile("testdb_adv_", ".sqlite");
            tmpFile.deleteOnExit();
            return "jdbc:sqlite:" + tmpFile.getAbsolutePath();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            uniqueSqliteUrl(),
            JdbcDriver.Sqlite
        );
        
        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "sqlite-advanced-test-queue"
        );
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        var queue = DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
        
        queues.add(queue);
        return queue;
    }
    
    private DelayedQueueJDBC<String> createQueueOnSameDB(String url, String tableName, MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.Sqlite
        );
        
        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "sqlite-shared-db-test-queue-" + tableName
        );
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        var queue = DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
        
        queues.add(queue);
        return queue;
    }
    
    @Test
    public void queuesWorkIndependently_whenUsingDifferentTableNames() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        // SQLite file-based DB for shared access across two queue instances
        var dbUrl = uniqueSqliteUrl();
        try (
            var queue1 = createQueueOnSameDB(dbUrl, "queue1", clock);
            var queue2 = createQueueOnSameDB(dbUrl, "queue2", clock)
        ) {

            var now = clock.now();
            var exitLater = now.plusSeconds(3600);
            var exitFirst = now.minusSeconds(10);
            var exitSecond = now.minusSeconds(5);

            // Insert 4 messages in each queue
            assertInstanceOf(OfferOutcome.Created.class,
                queue1.offerIfNotExists("key-1", "value 1 in queue 1", exitFirst));
            assertInstanceOf(OfferOutcome.Created.class,
                queue1.offerIfNotExists("key-2", "value 2 in queue 1", exitSecond));
            assertInstanceOf(OfferOutcome.Created.class,
                queue2.offerIfNotExists("key-1", "value 1 in queue 2", exitFirst));
            assertInstanceOf(OfferOutcome.Created.class,
                queue2.offerIfNotExists("key-2", "value 2 in queue 2", exitSecond));

            assertInstanceOf(OfferOutcome.Created.class,
                queue1.offerIfNotExists("key-3", "value 3 in queue 1", exitLater));
            assertInstanceOf(OfferOutcome.Created.class,
                queue1.offerIfNotExists("key-4", "value 4 in queue 1", exitLater));
            assertInstanceOf(OfferOutcome.Created.class,
                queue2.offerIfNotExists("key-3", "value 3 in queue 2", exitLater));
            assertInstanceOf(OfferOutcome.Created.class,
                queue2.offerIfNotExists("key-4", "value 4 in queue 2", exitLater));

            // Verify all messages exist
            assertTrue(queue1.containsMessage("key-1"));
            assertTrue(queue1.containsMessage("key-2"));
            assertTrue(queue1.containsMessage("key-3"));
            assertTrue(queue1.containsMessage("key-4"));
            assertTrue(queue2.containsMessage("key-1"));
            assertTrue(queue2.containsMessage("key-2"));
            assertTrue(queue2.containsMessage("key-3"));
            assertTrue(queue2.containsMessage("key-4"));

            // Update messages 2 and 4
            assertInstanceOf(OfferOutcome.Ignored.class,
                queue1.offerIfNotExists("key-1", "value 1 in queue 1 Updated", exitSecond));
            assertInstanceOf(OfferOutcome.Updated.class,
                queue1.offerOrUpdate("key-2", "value 2 in queue 1 Updated", exitSecond));
            assertInstanceOf(OfferOutcome.Ignored.class,
                queue1.offerIfNotExists("key-3", "value 3 in queue 1 Updated", exitLater));
            assertInstanceOf(OfferOutcome.Updated.class,
                queue1.offerOrUpdate("key-4", "value 4 in queue 1 Updated", exitLater));

            assertInstanceOf(OfferOutcome.Ignored.class,
                queue2.offerIfNotExists("key-1", "value 1 in queue 2 Updated", exitSecond));
            assertInstanceOf(OfferOutcome.Updated.class,
                queue2.offerOrUpdate("key-2", "value 2 in queue 2 Updated", exitSecond));
            assertInstanceOf(OfferOutcome.Ignored.class,
                queue2.offerIfNotExists("key-3", "value 3 in queue 2 Updated", exitLater));
            assertInstanceOf(OfferOutcome.Updated.class,
                queue2.offerOrUpdate("key-4", "value 4 in queue 2 Updated", exitLater));

            // Extract messages 1 and 2 from both queues
            var msg1InQ1 = queue1.tryPoll();
            assertNotNull(msg1InQ1);
            assertEquals("value 1 in queue 1", msg1InQ1.payload());
            msg1InQ1.acknowledge();

            var msg2InQ1 = queue1.tryPoll();
            assertNotNull(msg2InQ1);
            assertEquals("value 2 in queue 1 Updated", msg2InQ1.payload());
            msg2InQ1.acknowledge();

            var noMessageInQ1 = queue1.tryPoll();
            assertNull(noMessageInQ1);

            var msg1InQ2 = queue2.tryPoll();
            assertNotNull(msg1InQ2);
            assertEquals("value 1 in queue 2", msg1InQ2.payload());
            msg1InQ2.acknowledge();

            var msg2InQ2 = queue2.tryPoll();
            assertNotNull(msg2InQ2);
            assertEquals("value 2 in queue 2 Updated", msg2InQ2.payload());
            msg2InQ2.acknowledge();

            var noMessageInQ2 = queue2.tryPoll();
            assertNull(noMessageInQ2);

            // Verify only keys 3 and 4 are left
            assertFalse(queue1.containsMessage("key-1"));
            assertFalse(queue1.containsMessage("key-2"));
            assertTrue(queue1.containsMessage("key-3"));
            assertTrue(queue1.containsMessage("key-4"));
            assertFalse(queue2.containsMessage("key-1"));
            assertFalse(queue2.containsMessage("key-2"));
            assertTrue(queue2.containsMessage("key-3"));
            assertTrue(queue2.containsMessage("key-4"));

            // Drop all from Q1, verify Q2 is unaffected
            assertEquals(2, queue1.dropAllMessages("Yes, please, I know what I'm doing!"));
            assertTrue(queue2.containsMessage("key-3"));

            // Drop all from Q2
            assertEquals(2, queue2.dropAllMessages("Yes, please, I know what I'm doing!"));
            assertFalse(queue1.containsMessage("key-3"));
            assertFalse(queue2.containsMessage("key-3"));
        }
    }
    
    @Test
    public void concurrency_multipleProducersAndConsumers() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        try (var queue = createQueue("delayed_queue_test", clock)) {
            var now = clock.now();
            var messageCount = 200;
            var workers = 4;

            // Track created messages
            var createdCount = new AtomicInteger(0);
            var producerLatch = new CountDownLatch(workers);

            // Producers
            var producerThreads = new java.util.ArrayList<Thread>();
            for (int workerId = 0; workerId < workers; workerId++) {
                var thread = new Thread(() -> {
                    try {
                        for (int i = 0; i < messageCount; i++) {
                            var key = String.valueOf(i);
                            var result = queue.offerIfNotExists(key, key, now);
                            if (result instanceof OfferOutcome.Created) {
                                createdCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        producerLatch.countDown();
                    }
                });
                producerThreads.add(thread);
            }

            // Start all producers
            for (var thread : producerThreads) {
                thread.start();
            }

            // Wait for producers to finish
            assertTrue(producerLatch.await(30, TimeUnit.SECONDS));

            // Track consumed messages
            var consumedMessages = ConcurrentHashMap.<String>newKeySet();
            var consumerLatch = new CountDownLatch(workers);

            // Consumers
            var consumerThreads = new java.util.ArrayList<Thread>();
            for (int i = 0; i < workers; i++) {
                var thread = new Thread(() -> {
                    try {
                        while (true) {
                            var msg = queue.tryPoll();
                            if (msg == null) {
                                break;
                            }
                            consumedMessages.add(msg.payload());
                            msg.acknowledge();
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        consumerLatch.countDown();
                    }
                });
                consumerThreads.add(thread);
            }

            // Start all consumers
            for (var thread : consumerThreads) {
                thread.start();
            }

            // Wait for consumers to finish
            assertTrue(consumerLatch.await(30, TimeUnit.SECONDS));

            // Verify all messages were consumed
            assertEquals(messageCount, createdCount.get());
            assertEquals(messageCount, consumedMessages.size());

            // Verify queue is empty
            assertEquals(0, queue.dropAllMessages("Yes, please, I know what I'm doing!"));
        }
    }
}
