package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Comprehensive contract tests for CronService functionality.
 * Tests only the synchronous API, no background scheduling (which requires Thread.sleep).
 */
abstract class CronServiceContractTest {
    protected abstract fun createQueue(clock: TestClock): DelayedQueue<String>

    protected abstract fun cleanup()

    protected lateinit var queue: DelayedQueue<String>
    protected val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))

    @AfterEach
    fun baseCleanup() {
        cleanup()
    }

    @Test
    fun `installTick creates cron messages`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("test-config")

        val messages =
            listOf(
                CronMessage("payload1", clock.instant().plusSeconds(10)),
                CronMessage("payload2", clock.instant().plusSeconds(20)),
            )

        cron.installTick(configHash, "test-prefix", messages)

        // Both messages should exist
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "test-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "test-prefix", clock.instant().plusSeconds(20))
            )
        )
    }

    @Test
    fun `installTick replaces old configuration`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("replace-config")

        // First installation
        cron.installTick(
            configHash,
            "replace-prefix",
            listOf(
                CronMessage("old1", clock.instant().plusSeconds(10)),
                CronMessage("old2", clock.instant().plusSeconds(20)),
            ),
        )

        // Second installation with same hash should replace
        cron.installTick(
            configHash,
            "replace-prefix",
            listOf(
                CronMessage("new1", clock.instant().plusSeconds(15)),
                CronMessage("new2", clock.instant().plusSeconds(25)),
            ),
        )

        // Old messages should be gone
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "replace-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "replace-prefix", clock.instant().plusSeconds(20))
            )
        )

        // New messages should exist
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "replace-prefix", clock.instant().plusSeconds(15))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "replace-prefix", clock.instant().plusSeconds(25))
            )
        )
    }

    @Test
    fun `uninstallTick removes cron messages`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("uninstall-config")

        cron.installTick(
            configHash,
            "uninstall-prefix",
            listOf(
                CronMessage("payload1", clock.instant().plusSeconds(10)),
                CronMessage("payload2", clock.instant().plusSeconds(20)),
            ),
        )

        cron.uninstallTick(configHash, "uninstall-prefix")

        // Both messages should be gone
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "uninstall-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "uninstall-prefix", clock.instant().plusSeconds(20))
            )
        )
    }

    @Test
    fun `installTick with scheduleAtActual delays execution`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("delayed-config")

        val futureTime = clock.instant().plusSeconds(100)
        cron.installTick(
            configHash,
            "delayed-prefix",
            listOf(CronMessage("delayed-payload", futureTime)),
            scheduleAtActual = futureTime.minusSeconds(30), // Schedule 30 seconds before actual
        )

        val key = CronMessage.key(configHash, "delayed-prefix", futureTime)

        // Message should exist
        assertTrue(queue.containsMessage(key))

        // Check the scheduled time
        val msg = queue.read(key)
        assertNotNull(msg)

        // The message should be scheduled 30 seconds before the actual time
        // (scheduleAtActual parameter)
        // We can verify by checking when tryPoll returns it
        clock.advanceSeconds(25)
        assertNull(queue.tryPoll(), "Message should not be available yet")

        clock.advanceSeconds(10) // Now at +35 seconds
        val polled = queue.tryPoll()
        assertNotNull(polled, "Message should be available now")
        assertEquals("delayed-payload", polled!!.payload)
    }

    @Test
    fun `cron messages can be polled and acknowledged`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("poll-config")

        cron.installTick(
            configHash,
            "poll-prefix",
            listOf(CronMessage("pollable", clock.instant().minusSeconds(10))),
        )

        val msg = queue.tryPoll()
        assertNotNull(msg)
        assertEquals("pollable", msg!!.payload)

        // Acknowledge it
        queue.ack(msg)

        // Should be gone
        assertNull(queue.tryPoll())
    }

    @Test
    fun `different prefixes create separate messages`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("multi-prefix")

        cron.installTick(
            configHash,
            "prefix-a",
            listOf(CronMessage("payload-a", clock.instant().plusSeconds(10))),
        )
        cron.installTick(
            configHash,
            "prefix-b",
            listOf(CronMessage("payload-b", clock.instant().plusSeconds(10))),
        )

        // Both should exist with different keys
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "prefix-a", clock.instant().plusSeconds(10))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "prefix-b", clock.instant().plusSeconds(10))
            )
        )
    }

    @Test
    fun `cron messages with same time and different payloads update correctly`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("update-config")
        val scheduleTime = clock.instant().plusSeconds(10)

        // First installation
        cron.installTick(
            configHash,
            "update-prefix",
            listOf(CronMessage("original", scheduleTime)),
        )

        // Update with different payload at same time
        cron.installTick(
            configHash,
            "update-prefix",
            listOf(CronMessage("updated", scheduleTime)),
        )

        // Should have the updated payload
        val key = CronMessage.key(configHash, "update-prefix", scheduleTime)
        val msg = queue.read(key)
        assertNotNull(msg)
        assertEquals("updated", msg!!.payload)
    }

    @Test
    fun `installTick with empty list removes all messages`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("empty-config")

        // Install some messages
        cron.installTick(
            configHash,
            "empty-prefix",
            listOf(
                CronMessage("msg1", clock.instant().plusSeconds(10)),
                CronMessage("msg2", clock.instant().plusSeconds(20)),
            ),
        )

        // Install empty list
        cron.installTick(configHash, "empty-prefix", emptyList())

        // All messages should be gone
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "empty-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "empty-prefix", clock.instant().plusSeconds(20))
            )
        )
    }

    @Test
    fun `multiple configurations coexist independently`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val hash1 = CronConfigHash.fromString("config-1")
        val hash2 = CronConfigHash.fromString("config-2")

        cron.installTick(
            hash1,
            "prefix-1",
            listOf(CronMessage("payload-1", clock.instant().plusSeconds(10))),
        )
        cron.installTick(
            hash2,
            "prefix-2",
            listOf(CronMessage("payload-2", clock.instant().plusSeconds(10))),
        )

        // Both should exist
        assertTrue(
            queue.containsMessage(CronMessage.key(hash1, "prefix-1", clock.instant().plusSeconds(10)))
        )
        assertTrue(
            queue.containsMessage(CronMessage.key(hash2, "prefix-2", clock.instant().plusSeconds(10)))
        )

        // Uninstall first one
        cron.uninstallTick(hash1, "prefix-1")

        // First should be gone, second should remain
        assertFalse(
            queue.containsMessage(CronMessage.key(hash1, "prefix-1", clock.instant().plusSeconds(10)))
        )
        assertTrue(
            queue.containsMessage(CronMessage.key(hash2, "prefix-2", clock.instant().plusSeconds(10)))
        )
    }
}
