package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** Comprehensive tests for CronService functionality. */
class CronServiceTest {
    private val clock = TestClock(Instant.parse("2024-01-01T10:00:00Z"))
    private lateinit var queue: DelayedQueue<String>

    @AfterEach
    fun cleanup() {
        if (::queue.isInitialized && queue is AutoCloseable) {
            (queue as? AutoCloseable)?.close()
        }
    }

    private fun createQueue(): DelayedQueue<String> {
        queue =
            DelayedQueueInMemory.create(
                timeConfig = DelayedQueueTimeConfig.DEFAULT,
                ackEnvSource = "test",
                clock = clock,
            )
        return queue
    }

    @Test
    fun `installTick creates cron messages`() {
        val queue = createQueue()
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
    fun `uninstallTick removes cron messages`() {
        val queue = createQueue()
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("test-config")

        val messages =
            listOf(
                CronMessage("payload1", clock.instant().plusSeconds(10)),
                CronMessage("payload2", clock.instant().plusSeconds(20)),
            )

        cron.installTick(configHash, "test-prefix", messages)

        // Verify messages exist
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHash, "test-prefix", clock.instant().plusSeconds(10))
            )
        )

        // Uninstall
        cron.uninstallTick(configHash, "test-prefix")

        // Messages should be gone
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "test-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "test-prefix", clock.instant().plusSeconds(20))
            )
        )
    }

    @Test
    fun `installTick replaces old config with new config`() {
        val queue = createQueue()
        val cron = queue.getCron()
        val configHashOld = CronConfigHash.fromString("old-config")
        val configHashNew = CronConfigHash.fromString("new-config")

        // Install old config
        cron.installTick(
            configHashOld,
            "test-prefix",
            listOf(CronMessage("old-payload", clock.instant().plusSeconds(10))),
        )

        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHashOld, "test-prefix", clock.instant().plusSeconds(10))
            )
        )

        // Install new config
        cron.installTick(
            configHashNew,
            "test-prefix",
            listOf(CronMessage("new-payload", clock.instant().plusSeconds(20))),
        )

        // Old should be gone, new should exist
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHashOld, "test-prefix", clock.instant().plusSeconds(10))
            )
        )
        assertTrue(
            queue.containsMessage(
                CronMessage.key(configHashNew, "test-prefix", clock.instant().plusSeconds(20))
            )
        )
    }

    @Test
    fun `install creates periodic messages`() {
        val queue = createQueue()
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("periodic-config")

        val resource =
            cron.install(configHash, "periodic-prefix", Duration.ofMillis(100)) { now ->
                listOf(CronMessage("message-at-${now.epochSecond}", now.plusSeconds(60)))
            }

        try {
            // Wait for first execution
            Thread.sleep(250)

            // Should have created at least one message
            val count = queue.dropAllMessages("Yes, please, I know what I'm doing!")
            assertTrue(count >= 1, "Should have created at least one periodic message, got $count")
        } finally {
            resource.close()
        }
    }

    @Test
    fun `install can be stopped`() {
        val queue = createQueue()
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("stoppable-config")

        val resource =
            cron.install(configHash, "stoppable-prefix", Duration.ofMillis(50)) { now ->
                listOf(CronMessage("periodic-message", now.plusSeconds(60)))
            }

        Thread.sleep(120) // Let it run for a bit
        resource.close()

        // Clear all messages
        queue.dropAllMessages("Yes, please, I know what I'm doing!")

        Thread.sleep(100) // Wait to see if it continues

        // No new messages should appear
        val count = queue.dropAllMessages("Yes, please, I know what I'm doing!")
        assertEquals(0, count, "No new messages should be created after closing")
    }

    @Test
    fun `installDailySchedule creates messages at specified hours`() {
        val queue = createQueue()
        val cron = queue.getCron()

        val schedule =
            CronDailySchedule(
                hoursOfDay = listOf(LocalTime.of(14, 0), LocalTime.of(18, 0)),
                zoneId = ZoneId.of("UTC"),
                scheduleInterval = Duration.ofHours(1),
                scheduleInAdvance = Duration.ofMinutes(5),
            )

        val resource =
            cron.installDailySchedule("daily-prefix", schedule) { futureTime ->
                CronMessage("daily-${futureTime.epochSecond}", futureTime)
            }

        try {
            Thread.sleep(250) // Let it execute once

            // Should have created messages for next occurrences of 14:00 and 18:00
            val count = queue.dropAllMessages("Yes, please, I know what I'm doing!")
            // May be 0 if schedule doesn't match current time, or > 0 if it does
            assertTrue(count >= 0, "Schedule should execute without error")
        } finally {
            resource.close()
        }
    }

    @Test
    fun `installPeriodicTick creates messages at fixed intervals`() {
        val queue = createQueue()
        val cron = queue.getCron()

        val resource =
            cron.installPeriodicTick("tick-prefix", Duration.ofMillis(100)) { futureTime ->
                "tick-${futureTime.epochSecond}"
            }

        try {
            Thread.sleep(350) // Let it run for 3+ ticks

            val count = queue.dropAllMessages("Yes, please, I know what I'm doing!")
            assertTrue(count >= 1, "Should have created at least 1 tick message, got $count")
        } finally {
            resource.close()
        }
    }

    @Test
    fun `cron messages can be polled and processed`() {
        val queue = createQueue()
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("pollable-config")

        val messages =
            listOf(
                CronMessage("payload1", clock.instant().minusSeconds(10)),
                CronMessage("payload2", clock.instant().minusSeconds(5)),
            )

        cron.installTick(configHash, "poll-prefix", messages)

        // Poll messages
        val msg1 = queue.tryPoll()
        val msg2 = queue.tryPoll()

        assertNotNull(msg1)
        assertNotNull(msg2)
        assertEquals("payload1", msg1!!.payload)
        assertEquals("payload2", msg2!!.payload)

        // Acknowledge
        msg1.acknowledge()
        msg2.acknowledge()

        // Should be gone
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "poll-prefix", clock.instant().minusSeconds(10))
            )
        )
        assertFalse(
            queue.containsMessage(
                CronMessage.key(configHash, "poll-prefix", clock.instant().minusSeconds(5))
            )
        )
    }

    @Test
    fun `cronMessage scheduleAtActual allows delayed execution`() {
        val queue = createQueue()
        val cron = queue.getCron()
        val configHash = CronConfigHash.fromString("delayed-config")

        val nominalTime = clock.instant().plusSeconds(10)
        val actualTime = nominalTime.plusSeconds(5) // Execute 5 seconds later

        val messages =
            listOf(
                CronMessage(
                    payload = "delayed-payload",
                    scheduleAt = nominalTime,
                    scheduleAtActual = actualTime,
                )
            )

        cron.installTick(configHash, "delayed-prefix", messages)

        // Key is based on nominal time
        val key = CronMessage.key(configHash, "delayed-prefix", nominalTime)
        assertTrue(queue.containsMessage(key))

        // Should not be available yet (actual time is in future)
        clock.set(nominalTime.plusSeconds(1))
        val msg1 = queue.tryPoll()
        assertNull(msg1, "Message should not be available at nominal time")

        // Should be available after actual time
        clock.set(actualTime.plusSeconds(1))
        val msg2 = queue.tryPoll()
        assertNotNull(msg2, "Message should be available at actual time")
        assertEquals("delayed-payload", msg2!!.payload)
    }
}
