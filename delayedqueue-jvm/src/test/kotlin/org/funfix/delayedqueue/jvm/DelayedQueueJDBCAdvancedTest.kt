package org.funfix.delayedqueue.jvm

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Advanced JDBC-specific tests including concurrency and multi-queue isolation. These tests are
 * designed to be FAST - no artificial delays.
 */
class DelayedQueueJDBCAdvancedTest {
    private val queues = mutableListOf<DelayedQueueJDBC<String>>()

    @AfterEach
    fun cleanup() {
        queues.forEach { queue ->
            try {
                queue.dropAllMessages("Yes, please, I know what I'm doing!")
                queue.close()
            } catch (e: Exception) {
                // Ignore cleanup errors
            }
        }
        queues.clear()
    }

    private fun createQueue(
        tableName: String = "delayed_queue_test",
        clock: TestClock = TestClock(),
    ): DelayedQueueJDBC<String> {
        val config =
            JdbcConnectionConfig(
                url = "jdbc:hsqldb:mem:testdb_advanced_${System.currentTimeMillis()}",
                driver = JdbcDriver.HSQLDB,
                username = "SA",
                password = "",
                pool = null,
            )

        val queue =
            DelayedQueueJDBC.create(
                connectionConfig = config,
                tableName = tableName,
                serializer = MessageSerializer.forStrings(),
                timeConfig = DelayedQueueTimeConfig.DEFAULT,
                clock = clock,
            )

        queues.add(queue)
        return queue
    }

    private fun createQueueOnSameDB(
        url: String,
        tableName: String,
        clock: TestClock = TestClock(),
    ): DelayedQueueJDBC<String> {
        val config =
            JdbcConnectionConfig(
                url = url,
                driver = JdbcDriver.HSQLDB,
                username = "SA",
                password = "",
                pool = null,
            )

        val queue =
            DelayedQueueJDBC.create(
                connectionConfig = config,
                tableName = tableName,
                serializer = MessageSerializer.forStrings(),
                timeConfig = DelayedQueueTimeConfig.DEFAULT,
                clock = clock,
            )

        queues.add(queue)
        return queue
    }

    @Test
    fun `queues work independently when using different table names`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val dbUrl = "jdbc:hsqldb:mem:shared_db_${System.currentTimeMillis()}"
        val queue1 = createQueueOnSameDB(dbUrl, "queue1", clock)
        val queue2 = createQueueOnSameDB(dbUrl, "queue2", clock)

        val now = clock.instant()
        val exitLater = now.plusSeconds(3600)
        val exitFirst = now.minusSeconds(10)
        val exitSecond = now.minusSeconds(5)

        // Insert 4 messages in each queue
        assertEquals(
            OfferOutcome.Created,
            queue1.offerIfNotExists("key-1", "value 1 in queue 1", exitFirst),
        )
        assertEquals(
            OfferOutcome.Created,
            queue1.offerIfNotExists("key-2", "value 2 in queue 1", exitSecond),
        )
        assertEquals(
            OfferOutcome.Created,
            queue2.offerIfNotExists("key-1", "value 1 in queue 2", exitFirst),
        )
        assertEquals(
            OfferOutcome.Created,
            queue2.offerIfNotExists("key-2", "value 2 in queue 2", exitSecond),
        )

        assertEquals(
            OfferOutcome.Created,
            queue1.offerIfNotExists("key-3", "value 3 in queue 1", exitLater),
        )
        assertEquals(
            OfferOutcome.Created,
            queue1.offerIfNotExists("key-4", "value 4 in queue 1", exitLater),
        )
        assertEquals(
            OfferOutcome.Created,
            queue2.offerIfNotExists("key-3", "value 3 in queue 2", exitLater),
        )
        assertEquals(
            OfferOutcome.Created,
            queue2.offerIfNotExists("key-4", "value 4 in queue 2", exitLater),
        )

        // Verify all messages exist
        assertTrue(queue1.containsMessage("key-1"))
        assertTrue(queue1.containsMessage("key-2"))
        assertTrue(queue1.containsMessage("key-3"))
        assertTrue(queue1.containsMessage("key-4"))
        assertTrue(queue2.containsMessage("key-1"))
        assertTrue(queue2.containsMessage("key-2"))
        assertTrue(queue2.containsMessage("key-3"))
        assertTrue(queue2.containsMessage("key-4"))

        // Update messages 2 and 4
        assertEquals(
            OfferOutcome.Ignored,
            queue1.offerIfNotExists("key-1", "value 1 in queue 1 Updated", exitSecond),
        )
        assertEquals(
            OfferOutcome.Updated,
            queue1.offerOrUpdate("key-2", "value 2 in queue 1 Updated", exitSecond),
        )
        assertEquals(
            OfferOutcome.Ignored,
            queue1.offerIfNotExists("key-3", "value 3 in queue 1 Updated", exitLater),
        )
        assertEquals(
            OfferOutcome.Updated,
            queue1.offerOrUpdate("key-4", "value 4 in queue 1 Updated", exitLater),
        )

        assertEquals(
            OfferOutcome.Ignored,
            queue2.offerIfNotExists("key-1", "value 1 in queue 2 Updated", exitSecond),
        )
        assertEquals(
            OfferOutcome.Updated,
            queue2.offerOrUpdate("key-2", "value 2 in queue 2 Updated", exitSecond),
        )
        assertEquals(
            OfferOutcome.Ignored,
            queue2.offerIfNotExists("key-3", "value 3 in queue 2 Updated", exitLater),
        )
        assertEquals(
            OfferOutcome.Updated,
            queue2.offerOrUpdate("key-4", "value 4 in queue 2 Updated", exitLater),
        )

        // Extract messages 1 and 2 from both queues
        val msg1InQ1 = queue1.tryPoll()
        assertNotNull(msg1InQ1)
        assertEquals("value 1 in queue 1", msg1InQ1!!.payload)
        msg1InQ1.acknowledge()

        val msg2InQ1 = queue1.tryPoll()
        assertNotNull(msg2InQ1)
        assertEquals("value 2 in queue 1 Updated", msg2InQ1!!.payload)
        msg2InQ1.acknowledge()

        val noMessageInQ1 = queue1.tryPoll()
        assertNull(noMessageInQ1, "Should not be able to poll anymore from Q1")

        val msg1InQ2 = queue2.tryPoll()
        assertNotNull(msg1InQ2)
        assertEquals("value 1 in queue 2", msg1InQ2!!.payload)
        msg1InQ2.acknowledge()

        val msg2InQ2 = queue2.tryPoll()
        assertNotNull(msg2InQ2)
        assertEquals("value 2 in queue 2 Updated", msg2InQ2!!.payload)
        msg2InQ2.acknowledge()

        val noMessageInQ2 = queue2.tryPoll()
        assertNull(noMessageInQ2, "Should not be able to poll anymore from Q2")

        // Verify only keys 3 and 4 are left
        assertFalse(queue1.containsMessage("key-1"))
        assertFalse(queue1.containsMessage("key-2"))
        assertTrue(queue1.containsMessage("key-3"))
        assertTrue(queue1.containsMessage("key-4"))
        assertFalse(queue2.containsMessage("key-1"))
        assertFalse(queue2.containsMessage("key-2"))
        assertTrue(queue2.containsMessage("key-3"))
        assertTrue(queue2.containsMessage("key-4"))

        // Drop all from Q1, verify Q2 is unaffected
        assertEquals(2, queue1.dropAllMessages("Yes, please, I know what I'm doing!"))
        assertTrue(queue2.containsMessage("key-3"), "Deletion in Q1 should not affect Q2")

        // Drop all from Q2
        assertEquals(2, queue2.dropAllMessages("Yes, please, I know what I'm doing!"))
        assertFalse(queue1.containsMessage("key-3"))
        assertFalse(queue2.containsMessage("key-3"))
    }

    @Test
    fun `concurrency test - multiple producers and consumers`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueue(clock = clock)
        val now = clock.instant()
        val messageCount = 200
        val workers = 4

        // Track created messages
        val createdCount = AtomicInteger(0)
        val producerLatch = CountDownLatch(workers)

        // Producers
        val producerThreads =
            (0 until workers).map { workerId ->
                Thread {
                    try {
                        for (i in 0 until messageCount) {
                            val key =
                                i.toString() // Same keys across all workers for concurrency test
                            val result = queue.offerIfNotExists(key, key, now)
                            if (result == OfferOutcome.Created) {
                                createdCount.incrementAndGet()
                            }
                        }
                    } finally {
                        producerLatch.countDown()
                    }
                }
            }

        // Start all producers
        producerThreads.forEach { it.start() }

        // Wait for producers to finish
        assertTrue(producerLatch.await(10, TimeUnit.SECONDS), "Producers should finish")

        // Track consumed messages
        val consumedMessages = ConcurrentHashMap.newKeySet<String>()
        val consumerLatch = CountDownLatch(workers)

        // Consumers
        val consumerThreads =
            (0 until workers).map {
                Thread {
                    try {
                        while (true) {
                            val msg = queue.tryPoll()
                            if (msg == null) {
                                // No more messages available
                                break
                            }
                            consumedMessages.add(msg.payload)
                            msg.acknowledge()
                        }
                    } finally {
                        consumerLatch.countDown()
                    }
                }
            }

        // Start all consumers
        consumerThreads.forEach { it.start() }

        // Wait for consumers to finish
        assertTrue(consumerLatch.await(10, TimeUnit.SECONDS), "Consumers should finish")

        // Verify all messages were consumed
        assertEquals(
            messageCount,
            createdCount.get(),
            "Should create exactly $messageCount messages",
        )
        assertEquals(
            messageCount,
            consumedMessages.size,
            "Should consume exactly $messageCount messages",
        )

        // Verify queue is empty
        assertEquals(0, queue.dropAllMessages("Yes, please, I know what I'm doing!"))
    }
}
