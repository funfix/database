package org.funfix.delayedqueue.jvm

/** Tests for DelayedQueueJDBC with HSQLDB using the shared contract. */
class DelayedQueueJDBCHSQLDBContractTest : DelayedQueueContractTest() {
    private var currentQueue: DelayedQueueJDBC<String>? = null

    override fun createQueue(): DelayedQueue<String> =
        createQueue(DelayedQueueTimeConfig.DEFAULT, TestClock())

    override fun createQueue(timeConfig: DelayedQueueTimeConfig): DelayedQueue<String> =
        createQueue(timeConfig, TestClock())

    override fun createQueueWithClock(clock: TestClock): DelayedQueue<String> =
        createQueue(DelayedQueueTimeConfig.DEFAULT, clock)

    override fun createQueueWithClock(
        clock: TestClock,
        timeConfig: DelayedQueueTimeConfig,
    ): DelayedQueue<String> = createQueue(timeConfig, clock)

    private fun createQueue(
        timeConfig: DelayedQueueTimeConfig,
        clock: TestClock,
    ): DelayedQueue<String> {
        val dbConfig =
            JdbcConnectionConfig(
                url = "jdbc:hsqldb:mem:testdb_${System.currentTimeMillis()}",
                driver = JdbcDriver.HSQLDB,
                username = "SA",
                password = "",
                pool = null,
            )

        val queueConfig =
            DelayedQueueJDBCConfig(db = dbConfig, time = timeConfig, queueName = "test-queue")

        val queue =
            DelayedQueueJDBC.create(
                tableName = "delayed_queue_test",
                serializer = MessageSerializer.forStrings(),
                config = queueConfig,
                clock = clock,
            )

        currentQueue = queue
        return queue
    }

    override fun cleanup() {
        currentQueue?.let { queue ->
            try {
                queue.dropAllMessages("Yes, please, I know what I'm doing!")
                queue.close()
            } catch (e: Exception) {
                // Ignore cleanup errors
            }
        }
        currentQueue = null
    }
}
