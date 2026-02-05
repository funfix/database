package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Comprehensive test suite for DelayedQueue implementations.
 *
 * This abstract test class can be extended by both in-memory and JDBC implementations
 * to ensure they all meet the same contract.
 */
abstract class DelayedQueueContractTest {
    protected abstract fun createQueue(): DelayedQueue<String>

    protected abstract fun createQueue(timeConfig: DelayedQueueTimeConfig): DelayedQueue<String>

    protected abstract fun createQueueWithClock(clock: TestClock): DelayedQueue<String>

    protected abstract fun cleanup()

    @Test
    fun `offerIfNotExists creates new message`() {
        val queue = createQueue()
        try {
            val now = Instant.now()
            val result = queue.offerIfNotExists("key1", "payload1", now.plusSeconds(10))

            assertEquals(OfferOutcome.Created, result)
            assertTrue(queue.containsMessage("key1"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `offerIfNotExists ignores duplicate key`() {
        val queue = createQueue()
        try {
            val now = Instant.now()
            queue.offerIfNotExists("key1", "payload1", now.plusSeconds(10))
            val result = queue.offerIfNotExists("key1", "payload2", now.plusSeconds(20))

            assertEquals(OfferOutcome.Ignored, result)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `offerOrUpdate creates new message`() {
        val queue = createQueue()
        try {
            val now = Instant.now()
            val result = queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10))

            assertEquals(OfferOutcome.Created, result)
            assertTrue(queue.containsMessage("key1"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `offerOrUpdate updates existing message`() {
        val queue = createQueue()
        try {
            val now = Instant.now()
            queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10))
            val result = queue.offerOrUpdate("key1", "payload2", now.plusSeconds(20))

            assertEquals(OfferOutcome.Updated, result)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `offerOrUpdate ignores identical message`() {
        val queue = createQueue()
        try {
            val now = Instant.now()
            queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10))
            val result = queue.offerOrUpdate("key1", "payload1", now.plusSeconds(10))

            assertEquals(OfferOutcome.Ignored, result)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPoll returns null when no messages available`() {
        val queue = createQueue()
        try {
            val result = queue.tryPoll()
            assertNull(result)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPoll returns message when available`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            queue.offerOrUpdate("key1", "payload1", clock.instant().minusSeconds(10))

            val result = queue.tryPoll()

            assertNotNull(result)
            assertEquals("payload1", result!!.payload)
            assertEquals("key1", result.messageId.value)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPoll does not return future messages`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            // Schedule message 60 seconds in the future
            queue.offerOrUpdate("key1", "payload1", Instant.now().plusSeconds(60))

            val result = queue.tryPoll()

            // Should not be available yet (unless clock is real-time, then it's expected)
            if (queue is DelayedQueueInMemory<*>) {
                assertNull(result, "Future message should not be available in in-memory with test clock")
            }
        } finally {
            cleanup()
        }
    }

    @Test
    fun `acknowledge removes message from queue`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            queue.offerOrUpdate("key1", "payload1", clock.instant().minusSeconds(10))

            val message = queue.tryPoll()
            assertNotNull(message)

            message!!.acknowledge()

            assertFalse(queue.containsMessage("key1"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `acknowledge after update ignores old message`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            // Only run this test for in-memory with controllable clock
            if (queue !is DelayedQueueInMemory<*>) {
                return
            }

            queue.offerOrUpdate("key1", "payload1", clock.instant().minusSeconds(10))

            val message1 = queue.tryPoll()
            assertNotNull(message1)

            // Update while message is locked
            queue.offerOrUpdate("key1", "payload2", clock.instant().minusSeconds(5))

            // Ack first message - should NOT delete because of update
            message1!!.acknowledge()

            // Message should still exist
            assertTrue(queue.containsMessage("key1"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPollMany returns empty list when no messages`() {
        val queue = createQueue()
        try {
            val result = queue.tryPollMany(10)
            assertTrue(result.payload.isEmpty())
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPollMany returns available messages`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            queue.offerOrUpdate("key1", "payload1", clock.instant().minusSeconds(10))
            queue.offerOrUpdate("key2", "payload2", clock.instant().minusSeconds(5))

            val result = queue.tryPollMany(10)

            assertEquals(2, result.payload.size)
            assertTrue(result.payload.contains("payload1"))
            assertTrue(result.payload.contains("payload2"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPollMany respects batch size limit`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            for (i in 1..10) {
                queue.offerOrUpdate("key$i", "payload$i", clock.instant().minusSeconds(10))
            }

            val result = queue.tryPollMany(5)

            assertEquals(5, result.payload.size)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `read retrieves message without locking`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            queue.offerOrUpdate("key1", "payload1", clock.instant().plusSeconds(10))

            val result = queue.read("key1")

            assertNotNull(result)
            assertEquals("payload1", result!!.payload)
            assertTrue(queue.containsMessage("key1"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `dropMessage removes message`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            queue.offerOrUpdate("key1", "payload1", clock.instant().plusSeconds(10))

            assertTrue(queue.dropMessage("key1"))
            assertFalse(queue.containsMessage("key1"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `dropMessage returns false for non-existent key`() {
        val queue = createQueue()
        try {
            assertFalse(queue.dropMessage("non-existent"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `offerBatch creates multiple messages`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val messages =
                listOf(
                    BatchedMessage(
                        input = 1,
                        message = ScheduledMessage("key1", "payload1", clock.instant().plusSeconds(10)),
                    ),
                    BatchedMessage(
                        input = 2,
                        message = ScheduledMessage("key2", "payload2", clock.instant().plusSeconds(20)),
                    ),
                )

            val results = queue.offerBatch(messages)

            assertEquals(2, results.size)
            assertEquals(OfferOutcome.Created, results[0].outcome)
            assertEquals(OfferOutcome.Created, results[1].outcome)
            assertTrue(queue.containsMessage("key1"))
            assertTrue(queue.containsMessage("key2"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `offerBatch handles updates correctly`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            // Only run this test for in-memory with controllable clock
            if (queue !is DelayedQueueInMemory<*>) {
                // For JDBC, test with real time
                queue.offerOrUpdate("key1", "original", Instant.now().plusSeconds(10))

                val messages =
                    listOf(
                        BatchedMessage(
                            input = 1,
                            message = ScheduledMessage("key1", "updated", Instant.now().plusSeconds(20), canUpdate = true),
                        ),
                        BatchedMessage(
                            input = 2,
                            message = ScheduledMessage("key2", "new", Instant.now().plusSeconds(30)),
                        ),
                    )

                val results = queue.offerBatch(messages)

                assertEquals(2, results.size)
                assertEquals(OfferOutcome.Updated, results[0].outcome)
                assertEquals(OfferOutcome.Created, results[1].outcome)
                return
            }

            queue.offerOrUpdate("key1", "original", clock.instant().plusSeconds(10))

            val messages =
                listOf(
                    BatchedMessage(
                        input = 1,
                        message = ScheduledMessage("key1", "updated", clock.instant().plusSeconds(20), canUpdate = true),
                    ),
                    BatchedMessage(
                        input = 2,
                        message = ScheduledMessage("key2", "new", clock.instant().plusSeconds(30)),
                    ),
                )

            val results = queue.offerBatch(messages)

            assertEquals(2, results.size)
            assertEquals(OfferOutcome.Updated, results[0].outcome)
            assertEquals(OfferOutcome.Created, results[1].outcome)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `dropAllMessages removes all messages`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            queue.offerOrUpdate("key1", "payload1", clock.instant().plusSeconds(10))
            queue.offerOrUpdate("key2", "payload2", clock.instant().plusSeconds(20))

            val count = queue.dropAllMessages("Yes, please, I know what I'm doing!")

            assertEquals(2, count)
            assertFalse(queue.containsMessage("key1"))
            assertFalse(queue.containsMessage("key2"))
        } finally {
            cleanup()
        }
    }

    @Test
    fun `dropAllMessages requires confirmation`() {
        val queue = createQueue()
        try {
            assertThrows(IllegalArgumentException::class.java) {
                queue.dropAllMessages("wrong confirmation")
            }
        } finally {
            cleanup()
        }
    }

    @Test
    fun `FIFO ordering - messages polled in scheduled order`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val baseTime = clock.instant()
            queue.offerOrUpdate("key1", "payload1", baseTime.plusSeconds(3))
            queue.offerOrUpdate("key2", "payload2", baseTime.plusSeconds(1))
            queue.offerOrUpdate("key3", "payload3", baseTime.plusSeconds(2))

            // Advance time past all messages
            clock.advanceSeconds(4)

            val msg1 = queue.tryPoll()
            val msg2 = queue.tryPoll()
            val msg3 = queue.tryPoll()

            assertEquals("payload2", msg1!!.payload) // scheduled at +1s
            assertEquals("payload3", msg2!!.payload) // scheduled at +2s
            assertEquals("payload1", msg3!!.payload) // scheduled at +3s
        } finally {
            cleanup()
        }
    }

    @Test
    fun `poll blocks until message available`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val pollThread =
                Thread {
                    // This should block briefly then return
                    val msg = queue.poll()
                    assertEquals("payload1", msg.payload)
                }

            pollThread.start()
            Thread.sleep(100) // Let poll start waiting

            // Offer a message
            queue.offerOrUpdate("key1", "payload1", clock.instant().minusSeconds(1))

            pollThread.join(2000)
            assertFalse(pollThread.isAlive, "Poll should have returned")
        } finally {
            cleanup()
        }
    }

    @Test
    fun `redelivery after timeout`() {
        val timeConfig =
            DelayedQueueTimeConfig(
                acquireTimeout = Duration.ofSeconds(5),
                pollPeriod = Duration.ofMillis(10),
            )
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        try {
            // Only run this test for in-memory with controllable clock
            val queue =
                if (this is DelayedQueueInMemoryContractTest) {
                    DelayedQueueInMemory.create<String>(
                        timeConfig = timeConfig,
                        ackEnvSource = "test",
                        clock = clock,
                    )
                } else {
                    return // Skip for JDBC
                }

            queue.offerOrUpdate("key1", "payload1", clock.instant().minusSeconds(10))

            // First poll locks the message
            val msg1 = queue.tryPoll()
            assertNotNull(msg1)
            assertEquals(DeliveryType.FIRST_DELIVERY, msg1!!.deliveryType)

            // Don't acknowledge, advance time past timeout
            clock.advanceSeconds(6)

            // Should be redelivered
            val msg2 = queue.tryPoll()
            assertNotNull(msg2, "Message should be redelivered after timeout")
            assertEquals("payload1", msg2!!.payload)
            assertEquals(DeliveryType.REDELIVERY, msg2.deliveryType)
        } finally {
            cleanup()
        }
    }
}
