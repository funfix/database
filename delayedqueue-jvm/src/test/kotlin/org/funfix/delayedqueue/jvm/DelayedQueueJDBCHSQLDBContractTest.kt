package org.funfix.delayedqueue.jvm

/**
 * Tests for DelayedQueueJDBC with HSQLDB using the shared contract.
 */
class DelayedQueueJDBCHSQLDBContractTest : DelayedQueueContractTest() {
    private var currentQueue: DelayedQueueJDBC<String>? = null

    override fun createQueue(): DelayedQueue<String> =
        createQueue(DelayedQueueTimeConfig.DEFAULT)

    override fun createQueue(timeConfig: DelayedQueueTimeConfig): DelayedQueue<String> {
        val config =
            JdbcConnectionConfig(
                url = "jdbc:hsqldb:mem:testdb_${System.currentTimeMillis()}",
                driver = JdbcDriver.HSQLDB,
                username = "SA",
                password = "",
                pool = null,
            )

        val queue =
            DelayedQueueJDBC.create(
                connectionConfig = config,
                tableName = "delayed_queue_test",
                serializer = MessageSerializer.forStrings(),
                timeConfig = timeConfig,
            )

        currentQueue = queue
        return queue
    }

    override fun createQueueWithClock(clock: TestClock): DelayedQueue<String> {
        // JDBC implementation doesn't support clock injection
        // For clock-dependent tests, use in-memory instead or return regular queue
        return createQueue()
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
