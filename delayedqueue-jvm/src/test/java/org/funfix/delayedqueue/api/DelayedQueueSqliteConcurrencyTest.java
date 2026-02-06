package org.funfix.delayedqueue.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.funfix.delayedqueue.jvm.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Concurrency tests for DelayedQueueJDBC backed by SQLite to verify correct behavior
 * under concurrent access. Mirrors the existing HSQLDB concurrency tests.
 */
public class DelayedQueueSqliteConcurrencyTest {
    
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
            var tmpFile = java.io.File.createTempFile("testdb_conc_", ".sqlite");
            tmpFile.deleteOnExit();
            return "jdbc:sqlite:" + tmpFile.getAbsolutePath();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DelayedQueueJDBC<String> createQueue() throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            uniqueSqliteUrl(),
            JdbcDriver.Sqlite
        );
        
        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "sqlite-concurrency-test-queue");
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }
    
    /**
     * Test that concurrent offers on the same key produce exactly one Created outcome
     * and the rest are either Updated or Ignored (never duplicate Created).
     */
    @Test
    public void testConcurrentOffersOnSameKey() throws Exception {
        queue = createQueue();
        
        int numThreads = 10;
        String key = "concurrent-key";
        Instant scheduleAt = Instant.now().plusSeconds(60);
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<OfferOutcome>> futures = new ArrayList<>();
        
        // All threads try to offer with the same key but different payloads
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            futures.add(executor.submit(() -> {
                return queue.offerOrUpdate(key, "payload-" + threadId, scheduleAt);
            }));
        }
        
        // Collect results
        List<OfferOutcome> outcomes = new ArrayList<>();
        for (Future<OfferOutcome> future : futures) {
            outcomes.add(future.get(30, TimeUnit.SECONDS));
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        
        // Verify: Exactly one Created, rest are Updated or Ignored
        long createdCount = outcomes.stream().filter(o -> o instanceof OfferOutcome.Created).count();
        long updatedCount = outcomes.stream().filter(o -> o instanceof OfferOutcome.Updated).count();
        long ignoredCount = outcomes.stream().filter(o -> o instanceof OfferOutcome.Ignored).count();
        
        assertEquals(1, createdCount, "Exactly one thread should create the key");
        assertEquals(numThreads - 1, updatedCount + ignoredCount, "Other threads should update or ignore");
        
        // All outcomes should be valid
        assertEquals(numThreads, outcomes.size(), "All operations should complete");
    }
    
    /**
     * Test that concurrent offerIfNotExists on the same key produces exactly one Created
     * and the rest are Ignored (never duplicate Created).
     */
    @Test
    public void testConcurrentOfferIfNotExistsOnSameKey() throws Exception {
        queue = createQueue();
        
        int numThreads = 10;
        String key = "concurrent-no-update-key";
        Instant scheduleAt = Instant.now().plusSeconds(60);
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<OfferOutcome>> futures = new ArrayList<>();
        
        // All threads try to offerIfNotExists with the same key
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            futures.add(executor.submit(() -> {
                return queue.offerIfNotExists(key, "payload-" + threadId, scheduleAt);
            }));
        }
        
        // Collect results
        List<OfferOutcome> outcomes = new ArrayList<>();
        for (Future<OfferOutcome> future : futures) {
            outcomes.add(future.get(30, TimeUnit.SECONDS));
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        
        // Verify: Exactly one Created, rest are Ignored
        long createdCount = outcomes.stream().filter(o -> o instanceof OfferOutcome.Created).count();
        long ignoredCount = outcomes.stream().filter(o -> o instanceof OfferOutcome.Ignored).count();
        
        assertEquals(1, createdCount, "Exactly one thread should create the key");
        assertEquals(numThreads - 1, ignoredCount, "Other threads should be ignored");
    }
    
    /**
     * Test that concurrent polling delivers each message exactly once (no duplicates).
     */
    @Test
    public void testConcurrentPollingNoDuplicates() throws Exception {
        queue = createQueue();
        
        int numMessages = 50;
        int numThreads = 10;
        
        // Offer messages
        for (int i = 0; i < numMessages; i++) {
            queue.offerOrUpdate("msg-" + i, "payload-" + i, Instant.now());
        }
        
        // Poll concurrently
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<List<String>>> futures = new ArrayList<>();
        
        for (int t = 0; t < numThreads; t++) {
            futures.add(executor.submit(() -> {
                List<String> polled = new ArrayList<>();
                while (true) {
                    AckEnvelope<String> envelope = queue.tryPoll();
                    if (envelope == null) {
                        break;  // No more messages
                    }
                    polled.add(envelope.messageId().value());
                    envelope.acknowledge();
                }
                return polled;
            }));
        }
        
        // Collect all polled messages
        List<String> allPolled = new ArrayList<>();
        for (Future<List<String>> future : futures) {
            allPolled.addAll(future.get(60, TimeUnit.SECONDS));
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        
        // Verify: All messages polled exactly once
        assertEquals(numMessages, allPolled.size(), "All messages should be polled");
        assertEquals(numMessages, allPolled.stream().distinct().count(), "No duplicates allowed");
        
        // Verify all messages are there
        List<String> expected = IntStream.range(0, numMessages)
            .mapToObj(i -> "msg-" + i)
            .sorted()
            .collect(Collectors.toList());
        
        Collections.sort(allPolled);
        assertEquals(expected, allPolled, "All messages should be accounted for (by messageId=key)");
    }
    
    /**
     * Test that batch offers with concurrent individual offers don't cause exceptions.
     */
    @Test
    public void testBatchOfferWithConcurrentSingleOffers() throws Exception {
        queue = createQueue();
        
        int batchSize = 20;
        int numConcurrentOffers = 10;
        
        Instant scheduleAt = Instant.now().plusSeconds(60);
        
        // Prepare batch messages (keys 0-19)
        List<BatchedMessage<Integer, String>> batch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            batch.add(new BatchedMessage<>(
                i,
                new ScheduledMessage<>("batch-key-" + i, "batch-payload-" + i, scheduleAt, true)
            ));
        }
        
        // Start batch offer in one thread
        ExecutorService executor = Executors.newFixedThreadPool(numConcurrentOffers + 1);
        Future<List<BatchedReply<Integer, String>>> batchFuture = executor.submit(() -> {
            return queue.offerBatch(batch);
        });
        
        // Concurrently offer individual messages (some overlap with batch keys)
        List<Future<OfferOutcome>> singleOfferFutures = new ArrayList<>();
        for (int i = 0; i < numConcurrentOffers; i++) {
            final int keyIndex = i * 2;  // Keys 0, 2, 4, 6, ... (overlap with batch)
            singleOfferFutures.add(executor.submit(() -> {
                return queue.offerOrUpdate("batch-key-" + keyIndex, "single-payload-" + keyIndex, scheduleAt);
            }));
        }
        
        // Wait for all to complete (should not throw exceptions)
        List<BatchedReply<Integer, String>> batchResults = batchFuture.get(30, TimeUnit.SECONDS);
        for (Future<OfferOutcome> future : singleOfferFutures) {
            future.get(30, TimeUnit.SECONDS);
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        
        // Verify batch results are sane (all keys have an outcome)
        assertEquals(batchSize, batchResults.size());
        for (BatchedReply<Integer, String> reply : batchResults) {
            assertNotNull(reply.outcome());
        }
    }
    
    /**
     * Test that rapid offer updates on the same key don't lose updates or throw exceptions.
     */
    @Test
    public void testRapidOfferUpdatesOnSameKey() throws Exception {
        queue = createQueue();
        
        String key = "rapid-update-key";
        int numUpdates = 100;
        
        // Pre-create the key
        queue.offerOrUpdate(key, "initial", Instant.now().plusSeconds(60));
        
        // Rapidly update the same key from multiple threads
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<OfferOutcome>> futures = new ArrayList<>();
        
        for (int i = 0; i < numUpdates; i++) {
            final int updateId = i;
            futures.add(executor.submit(() -> {
                return queue.offerOrUpdate(key, "update-" + updateId, Instant.now().plusSeconds(updateId));
            }));
        }
        
        // All should complete without exceptions
        for (Future<OfferOutcome> future : futures) {
            OfferOutcome outcome = future.get(30, TimeUnit.SECONDS);
            assertNotNull(outcome);
            // Outcome can be Updated or Ignored (if concurrent modification detected)
            assertTrue(outcome instanceof OfferOutcome.Updated || outcome instanceof OfferOutcome.Ignored,
                "Outcome should be Updated or Ignored, but was: " + outcome);
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
}
