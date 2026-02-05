package org.funfix.delayedqueue.jvm

/** Tests for DelayedQueueInMemory using the shared contract. */
class DelayedQueueInMemoryContractTest : DelayedQueueContractTest() {
    override fun createQueue(): DelayedQueue<String> = DelayedQueueInMemory.create()

    override fun createQueue(timeConfig: DelayedQueueTimeConfig): DelayedQueue<String> =
        DelayedQueueInMemory.create(timeConfig)

    override fun createQueueWithClock(clock: TestClock): DelayedQueue<String> =
        DelayedQueueInMemory.create(
            timeConfig = DelayedQueueTimeConfig.DEFAULT,
            ackEnvSource = "test",
            clock = clock,
        )

    override fun createQueueWithClock(
        clock: TestClock,
        timeConfig: DelayedQueueTimeConfig,
    ): DelayedQueue<String> =
        DelayedQueueInMemory.create(timeConfig = timeConfig, ackEnvSource = "test", clock = clock)

    override fun cleanup() {
        // In-memory queue is garbage collected, no cleanup needed
    }
}
