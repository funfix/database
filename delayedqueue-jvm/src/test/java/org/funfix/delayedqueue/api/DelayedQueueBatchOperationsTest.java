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

import java.time.Instant;
import java.util.List;
import org.funfix.delayedqueue.jvm.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Java API tests for DelayedQueue batch operations.
 * <p>
 * Tests both in-memory and JDBC implementations to ensure batch insert/update
 * behaves correctly with duplicate keys and concurrent operations.
 */
public class DelayedQueueBatchOperationsTest {
    
    private DelayedQueue<String> queue;
    
    @AfterEach
    public void cleanup() {
        if (queue != null) {
            try {
                if (queue instanceof DelayedQueueJDBC<?> jdbcQueue) {
                    jdbcQueue.dropAllMessages("Yes, please, I know what I'm doing!");
                    jdbcQueue.close();
                }
                // In-memory queue doesn't need explicit cleanup
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }
    
    private DelayedQueue<String> createInMemoryQueue(MutableClock clock) {
        return DelayedQueueInMemory.create(
            DelayedQueueTimeConfig.DEFAULT_IN_MEMORY,
            "test-source",
            clock
        );
    }
    
    private DelayedQueue<String> createJdbcQueue(MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            "jdbc:hsqldb:mem:testdb_batch_" + System.currentTimeMillis(),
            JdbcDriver.HSQLDB,
            "SA",
            "",
            null
        );
        
        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_batch_test", "batch-test-queue");
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }
    
    // ========== In-Memory Tests ==========
    
    @Test
    public void inMemory_batchInsertWithDuplicateKeys_shouldFallbackCorrectly() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createInMemoryQueue(clock);
        var now = clock.now();
        
        // First, insert some messages individually
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-1", "initial-1", now));
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-3", "initial-3", now));
        
        // Now try batch insert with some duplicate keys
        var messages = List.of(
            new BatchedMessage<>(1, new ScheduledMessage<>("key-1", "batch-1", now, false)),
            new BatchedMessage<>(2, new ScheduledMessage<>("key-2", "batch-2", now, false)),
            new BatchedMessage<>(3, new ScheduledMessage<>("key-3", "batch-3", now, false)),
            new BatchedMessage<>(4, new ScheduledMessage<>("key-4", "batch-4", now, false))
        );
        
        var results = queue.offerBatch(messages);
        
        // Verify results
        assertEquals(4, results.size());
        
        // key-1 and key-3 already exist, should be ignored (canUpdate = false)
        var result1 = findResultByInput(results, 1);
        assertInstanceOf(OfferOutcome.Ignored.class, result1.outcome());
        
        var result3 = findResultByInput(results, 3);
        assertInstanceOf(OfferOutcome.Ignored.class, result3.outcome());
        
        // key-2 and key-4 should be created
        var result2 = findResultByInput(results, 2);
        assertInstanceOf(OfferOutcome.Created.class, result2.outcome());
        
        var result4 = findResultByInput(results, 4);
        assertInstanceOf(OfferOutcome.Created.class, result4.outcome());
        
        // Verify actual queue state
        assertTrue(queue.containsMessage("key-1"));
        assertTrue(queue.containsMessage("key-2"));
        assertTrue(queue.containsMessage("key-3"));
        assertTrue(queue.containsMessage("key-4"));
        
        // Verify the values weren't updated (canUpdate = false)
        var msg1 = queue.tryPoll();
        assertNotNull(msg1);
        assertEquals("initial-1", msg1.payload()); // Should still be initial value
        msg1.acknowledge();
    }
    
    @Test
    public void inMemory_batchInsertWithUpdatesAllowed_shouldHandleDuplicatesCorrectly() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createInMemoryQueue(clock);
        var now = clock.now();
        
        // Insert initial messages
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-1", "initial-1", now));
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-2", "initial-2", now));
        
        // Batch with updates allowed
        var messages = List.of(
            new BatchedMessage<>(1, new ScheduledMessage<>("key-1", "updated-1", now, true)),
            new BatchedMessage<>(2, new ScheduledMessage<>("key-2", "updated-2", now, true)),
            new BatchedMessage<>(3, new ScheduledMessage<>("key-3", "new-3", now, true))
        );
        
        var results = queue.offerBatch(messages);
        
        // Verify results - existing should be updated, new should be created
        assertEquals(3, results.size());
        
        var result1 = findResultByInput(results, 1);
        assertInstanceOf(OfferOutcome.Updated.class, result1.outcome());
        
        var result2 = findResultByInput(results, 2);
        assertInstanceOf(OfferOutcome.Updated.class, result2.outcome());
        
        var result3 = findResultByInput(results, 3);
        assertInstanceOf(OfferOutcome.Created.class, result3.outcome());
    }
    
    // ========== JDBC Tests ==========
    
    @Test
    public void jdbc_batchInsertWithDuplicateKeys_shouldFallbackCorrectly() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createJdbcQueue(clock);
        var now = clock.now();
        
        // First, insert some messages individually
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-1", "initial-1", now));
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-3", "initial-3", now));
        
        // Now try batch insert with some duplicate keys
        var messages = List.of(
            new BatchedMessage<>(1, new ScheduledMessage<>("key-1", "batch-1", now, false)),
            new BatchedMessage<>(2, new ScheduledMessage<>("key-2", "batch-2", now, false)),
            new BatchedMessage<>(3, new ScheduledMessage<>("key-3", "batch-3", now, false)),
            new BatchedMessage<>(4, new ScheduledMessage<>("key-4", "batch-4", now, false))
        );
        
        var results = queue.offerBatch(messages);
        
        // Verify results
        assertEquals(4, results.size());
        
        // key-1 and key-3 already exist, should be ignored (canUpdate = false)
        var result1 = findResultByInput(results, 1);
        assertInstanceOf(OfferOutcome.Ignored.class, result1.outcome());
        
        var result3 = findResultByInput(results, 3);
        assertInstanceOf(OfferOutcome.Ignored.class, result3.outcome());
        
        // key-2 and key-4 should be created
        var result2 = findResultByInput(results, 2);
        assertInstanceOf(OfferOutcome.Created.class, result2.outcome());
        
        var result4 = findResultByInput(results, 4);
        assertInstanceOf(OfferOutcome.Created.class, result4.outcome());
        
        // Verify actual queue state
        assertTrue(queue.containsMessage("key-1"));
        assertTrue(queue.containsMessage("key-2"));
        assertTrue(queue.containsMessage("key-3"));
        assertTrue(queue.containsMessage("key-4"));
        
        // Verify the values weren't updated (canUpdate = false)
        var msg1 = queue.tryPoll();
        assertNotNull(msg1);
        assertEquals("initial-1", msg1.payload()); // Should still be initial value
        msg1.acknowledge();
    }
    
    @Test
    public void jdbc_batchInsertWithUpdatesAllowed_shouldHandleDuplicatesCorrectly() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createJdbcQueue(clock);
        var now = clock.now();
        
        // Insert initial messages
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-1", "initial-1", now));
        assertInstanceOf(OfferOutcome.Created.class, queue.offerIfNotExists("key-2", "initial-2", now));
        
        // Batch with updates allowed
        var messages = List.of(
            new BatchedMessage<>(1, new ScheduledMessage<>("key-1", "updated-1", now, true)),
            new BatchedMessage<>(2, new ScheduledMessage<>("key-2", "updated-2", now, true)),
            new BatchedMessage<>(3, new ScheduledMessage<>("key-3", "new-3", now, true))
        );
        
        var results = queue.offerBatch(messages);
        
        // Verify results - existing should be updated, new should be created
        assertEquals(3, results.size());
        
        var result1 = findResultByInput(results, 1);
        assertInstanceOf(OfferOutcome.Updated.class, result1.outcome());
        
        var result2 = findResultByInput(results, 2);
        assertInstanceOf(OfferOutcome.Updated.class, result2.outcome());
        
        var result3 = findResultByInput(results, 3);
        assertInstanceOf(OfferOutcome.Created.class, result3.outcome());
    }

    @Test
    public void jdbc_batchInsertLargeWithExistingDuplicates_shouldInsertNonDuplicates() throws Exception {
        var clock = new MutableClock(Instant.parse("2024-01-01T10:00:00Z"));
        queue = createJdbcQueue(clock);
        var now = clock.now();

        // Seed existing keys that will be duplicated in the batch
        for (int i = 0; i < 10; i++) {
            assertInstanceOf(OfferOutcome.Created.class,
                queue.offerIfNotExists("key-" + i, "existing-" + i, now));
        }

        var messages = new java.util.ArrayList<BatchedMessage<Integer, String>>();
        for (int i = 0; i < 250; i++) {
            messages.add(new BatchedMessage<>(
                i,
                new ScheduledMessage<>("key-" + i, "batch-" + i, now, false)
            ));
        }

        var results = queue.offerBatch(messages);

        assertEquals(250, results.size());

        for (int i = 0; i < 10; i++) {
            var result = findResultByInput(results, i);
            assertInstanceOf(OfferOutcome.Ignored.class, result.outcome());
        }

        for (int i = 10; i < 250; i++) {
            var result = findResultByInput(results, i);
            assertInstanceOf(OfferOutcome.Created.class, result.outcome());
        }

        var existing = queue.read("key-0");
        assertNotNull(existing);
        assertEquals("existing-0", existing.payload());
    }
    
    // ========== Helper Methods ==========
    
    private <In, A> BatchedReply<In, A> findResultByInput(List<BatchedReply<In, A>> results, In input) {
        return results.stream()
            .filter(r -> r.input().equals(input))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Result not found for input: " + input));
    }
}
