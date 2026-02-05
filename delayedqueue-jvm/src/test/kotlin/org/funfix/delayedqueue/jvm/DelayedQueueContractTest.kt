package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Comprehensive test suite for DelayedQueue implementations.
 *
 * This abstract test class can be extended by both in-memory and JDBC implementations to ensure
 * they all meet the same contract.
 */
abstract class DelayedQueueContractTest {
    protected abstract fun createQueue(): DelayedQueue<String>

    protected abstract fun createQueue(timeConfig: DelayedQueueTimeConfig): DelayedQueue<String>

    protected abstract fun createQueueWithClock(clock: TestClock): DelayedQueue<String>

    protected open fun createQueueWithClock(
        clock: TestClock,
        timeConfig: DelayedQueueTimeConfig,
    ): DelayedQueue<String> = createQueueWithClock(clock)

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
                assertNull(
                    result,
                    "Future message should not be available in in-memory with test clock",
                )
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
                        message =
                            ScheduledMessage("key1", "payload1", clock.instant().plusSeconds(10)),
                    ),
                    BatchedMessage(
                        input = 2,
                        message =
                            ScheduledMessage("key2", "payload2", clock.instant().plusSeconds(20)),
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
                            message =
                                ScheduledMessage(
                                    "key1",
                                    "updated",
                                    Instant.now().plusSeconds(20),
                                    canUpdate = true,
                                ),
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
                        message =
                            ScheduledMessage(
                                "key1",
                                "updated",
                                clock.instant().plusSeconds(20),
                                canUpdate = true,
                            ),
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
            val pollThread = Thread {
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
            val queue = createQueueWithClock(clock, timeConfig)

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

    @Test
    fun `poll-ack only deletes if no update happened in-between`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val now = clock.instant()

            // Create a message and poll it
            val offer1 = queue.offerOrUpdate("my-key", "value offered (1)", now.minusSeconds(1))
            assertEquals(OfferOutcome.Created, offer1)

            val msg1 = queue.tryPoll()
            assertNotNull(msg1)
            assertEquals("value offered (1)", msg1!!.payload)

            // Update the message while holding the first poll
            val offer2 = queue.offerOrUpdate("my-key", "value offered (2)", now.minusSeconds(1))
            assertEquals(OfferOutcome.Updated, offer2)

            // Poll again to get the updated version
            val msg2 = queue.tryPoll()
            assertNotNull(msg2)
            assertEquals("value offered (2)", msg2!!.payload)

            // Acknowledge the first message - should have no effect because message was updated
            msg1.acknowledge()
            assertTrue(
                queue.containsMessage("my-key"),
                "Message should still exist after ack on stale version",
            )

            // Acknowledge the second message - should delete the message
            msg2.acknowledge()
            assertFalse(
                queue.containsMessage("my-key"),
                "Message should be deleted after ack on current version",
            )
        } finally {
            cleanup()
        }
    }

    @Test
    fun `read-ack only deletes if no update happened in-between`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val now = clock.instant()

            // Create three messages
            queue.offerOrUpdate("my-key-1", "value offered (1.1)", now.minusSeconds(1))
            queue.offerOrUpdate("my-key-2", "value offered (2.1)", now.minusSeconds(1))
            queue.offerOrUpdate("my-key-3", "value offered (3.1)", now.minusSeconds(1))

            // Read all three messages
            val msg1 = queue.read("my-key-1")
            val msg2 = queue.read("my-key-2")
            val msg3 = queue.read("my-key-3")
            val msg4 = queue.read("my-key-4")

            assertNotNull(msg1)
            assertNotNull(msg2)
            assertNotNull(msg3)
            assertNull(msg4)

            assertEquals("value offered (1.1)", msg1!!.payload)
            assertEquals("value offered (2.1)", msg2!!.payload)
            assertEquals("value offered (3.1)", msg3!!.payload)

            // Advance clock to ensure updates have different createdAt
            clock.advanceSeconds(1)

            // Update msg2 (payload) and msg3 (scheduleAt)
            queue.offerOrUpdate("my-key-2", "value offered (2.2)", now.minusSeconds(1))
            queue.offerOrUpdate("my-key-3", "value offered (3.1)", now)

            // Acknowledge all three messages
            // Only msg1 should be deleted (msg2 and msg3 were updated)
            msg1.acknowledge()
            msg2.acknowledge()
            msg3.acknowledge()

            assertFalse(queue.containsMessage("my-key-1"), "msg1 should be deleted")
            assertTrue(queue.containsMessage("my-key-2"), "msg2 should still exist (was updated)")
            assertTrue(queue.containsMessage("my-key-3"), "msg3 should still exist (was updated)")

            // Verify 2 messages remaining
            val remaining = queue.dropAllMessages("Yes, please, I know what I'm doing!")
            assertEquals(2, remaining)
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPollMany with batch size smaller than pagination`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val now = clock.instant()

            // Offer 50 messages
            val messages =
                (0 until 50).map { i ->
                    BatchedMessage(
                        input = i,
                        message =
                            ScheduledMessage(
                                key = "key-$i",
                                payload = "payload-$i",
                                scheduleAt = now.minusSeconds(50 - i.toLong()),
                                canUpdate = false,
                            ),
                    )
                }
            queue.offerBatch(messages)

            // Poll all 50 messages
            val batch = queue.tryPollMany(50)
            assertEquals(50, batch.payload.size, "Should get all 50 messages")

            // Verify order and content
            batch.payload.forEachIndexed { idx, msg ->
                assertEquals("payload-$idx", msg, "Messages should be in FIFO order")
            }

            batch.acknowledge()

            // Verify queue is empty
            val batch2 = queue.tryPollMany(10)
            assertTrue(batch2.payload.isEmpty(), "Queue should be empty")
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPollMany with batch size equal to pagination`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val now = clock.instant()

            // Offer 100 messages
            val messages =
                (0 until 100).map { i ->
                    BatchedMessage(
                        input = i,
                        message =
                            ScheduledMessage(
                                key = "key-$i",
                                payload = "payload-$i",
                                scheduleAt = now.minusSeconds(100 - i.toLong()),
                                canUpdate = false,
                            ),
                    )
                }
            queue.offerBatch(messages)

            // Poll all 100 messages
            val batch = queue.tryPollMany(100)
            assertEquals(100, batch.payload.size, "Should get all 100 messages")

            // Verify order and content
            batch.payload.forEachIndexed { idx, msg ->
                assertEquals("payload-$idx", msg, "Messages should be in FIFO order")
            }

            batch.acknowledge()

            // Verify queue is empty
            val batch2 = queue.tryPollMany(3)
            assertTrue(batch2.payload.isEmpty(), "Queue should be empty")
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPollMany with batch size larger than pagination`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val now = clock.instant()

            // Offer 250 messages
            val messages =
                (0 until 250).map { i ->
                    BatchedMessage(
                        input = i,
                        message =
                            ScheduledMessage(
                                key = "key-$i",
                                payload = "payload-$i",
                                scheduleAt = now.minusSeconds(250 - i.toLong()),
                                canUpdate = false,
                            ),
                    )
                }
            queue.offerBatch(messages)

            // Poll all 250 messages
            val batch = queue.tryPollMany(250)
            assertEquals(250, batch.payload.size, "Should get all 250 messages")

            // Verify order and content
            batch.payload.forEachIndexed { idx, msg ->
                assertEquals("payload-$idx", msg, "Messages should be in FIFO order")
            }

            batch.acknowledge()

            // Verify queue is empty
            val batch2 = queue.tryPollMany(10)
            assertTrue(batch2.payload.isEmpty(), "Queue should be empty")
        } finally {
            cleanup()
        }
    }

    @Test
    fun `tryPollMany with maxSize less than or equal to 0 returns empty batch`() {
        val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
        val queue = createQueueWithClock(clock)
        try {
            val now = clock.instant()

            queue.offerOrUpdate("my-key-1", "value offered (1.1)", now.minusSeconds(1))
            queue.offerOrUpdate("my-key-2", "value offered (2.1)", now.minusSeconds(2))

            // tryPollMany with <= 0 should just return empty, not throw
            // The implementation should handle this gracefully
            val batch0 = queue.tryPollMany(0)
            assertTrue(batch0.payload.isEmpty(), "maxSize=0 should return empty batch")
            batch0.acknowledge()

            // Verify messages are still in queue
            val batch3 = queue.tryPollMany(3)
            assertEquals(2, batch3.payload.size, "Should still have 2 messages")
            assertTrue(batch3.payload.contains("value offered (1.1)"))
            assertTrue(batch3.payload.contains("value offered (2.1)"))
        } finally {
            cleanup()
        }
    }
}
