package org.funfix.delayedqueue.jvm

/**
 * CronService contract tests for in-memory implementation.
 */
class CronServiceInMemoryContractTest : CronServiceContractTest() {
    override fun createQueue(clock: TestClock): DelayedQueue<String> =
        DelayedQueueInMemory.create(
            timeConfig = DelayedQueueTimeConfig.DEFAULT,
            ackEnvSource = "test-cron",
            clock = clock,
        )

    override fun cleanup() {
        // In-memory queue is garbage collected
    }
}
