package org.funfix.delayedqueue.jvm

import java.time.Instant
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Comprehensive contract tests for CronService functionality. Tests only the synchronous API, no
 * background scheduling (which requires Thread.sleep).
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
    fun `installTick replaces old configuration when hash changes`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val oldHash = CronConfigHash.fromString("old-config")
        val newHash = CronConfigHash.fromString("new-config")

        // First installation with old hash
        cron.installTick(
            oldHash,
            "replace-prefix",
            listOf(
                CronMessage("old1", clock.instant().plusSeconds(10)),
                CronMessage("old2", clock.instant().plusSeconds(20)),
            ),
        )

        // Second installation with NEW hash should replace old hash messages
        cron.installTick(
            newHash,
            "replace-prefix",
            listOf(
                CronMessage("new1", clock.instant().plusSeconds(15)),
                CronMessage("new2", clock.instant().plusSeconds(25)),
            ),
        )

        // Old messages with old hash should be gone
        assertFalse(
            queue.containsMessage(
                CronMessage.key(oldHash, "replace-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertFalse(
            queue.containsMessage(
                CronMessage.key(oldHash, "replace-prefix", clock.instant().plusSeconds(20))
            )
        )

        // New messages with new hash should exist
        assertTrue(
            queue.containsMessage(
                CronMessage.key(newHash, "replace-prefix", clock.instant().plusSeconds(15))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(newHash, "replace-prefix", clock.instant().plusSeconds(25))
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
    fun `installTick with same hash adds new messages`() {
        queue = createQueue(clock)
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("same-hash")

        // First installation
        cron.installTick(
            configHash,
            "same-prefix",
            listOf(CronMessage("msg1", clock.instant().plusSeconds(10))),
        )

        // Second installation with same hash - old messages are NOT deleted
        // (because they match the current hash)
        cron.installTick(
            configHash,
            "same-prefix",
            listOf(CronMessage("msg2", clock.instant().plusSeconds(20))),
        )

        // Both messages should exist (different timestamps = different keys)
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "same-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "same-prefix", clock.instant().plusSeconds(20))
            )
        )
    }

    @Test
    fun `installTick with empty list removes nothing when hash matches`() {
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

        // Install empty list with SAME hash - messages are NOT deleted
        cron.installTick(configHash, "empty-prefix", emptyList())

        // Messages should still exist (empty list just doesn't add anything)
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "empty-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "empty-prefix", clock.instant().plusSeconds(20))
            )
        )
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
        msg.acknowledge()

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
            queue.containsMessage(
                CronMessage.key(hash1, "prefix-1", clock.instant().plusSeconds(10))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(hash2, "prefix-2", clock.instant().plusSeconds(10))
            )
        )

        // Uninstall first one
        cron.uninstallTick(hash1, "prefix-1")

        // First should be gone, second should remain
        assertFalse(
            queue.containsMessage(
                CronMessage.key(hash1, "prefix-1", clock.instant().plusSeconds(10))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(hash2, "prefix-2", clock.instant().plusSeconds(10))
            )
        )
    }
}
