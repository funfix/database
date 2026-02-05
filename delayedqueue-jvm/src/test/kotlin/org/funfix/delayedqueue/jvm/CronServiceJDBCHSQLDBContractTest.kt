package org.funfix.delayedqueue.jvm

/** CronService contract tests for JDBC implementation with HSQLDB. */
class CronServiceJDBCHSQLDBContractTest : CronServiceContractTest() {
    private var currentQueue: DelayedQueueJDBC<String>? = null

    override fun createQueue(clock: TestClock): DelayedQueue<String> {
        val config =
            JdbcConnectionConfig(
                url = "jdbc:hsqldb:mem:crontest_${System.nanoTime()}",
                driver = JdbcDriver.HSQLDB,
                username = "SA",
                password = "",
                pool = null,
            )

        val queue =
            DelayedQueueJDBC.create(
                connectionConfig = config,
                tableName = "delayed_queue_cron_test",
                serializer = MessageSerializer.forStrings(),
                timeConfig = DelayedQueueTimeConfig.DEFAULT,
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
