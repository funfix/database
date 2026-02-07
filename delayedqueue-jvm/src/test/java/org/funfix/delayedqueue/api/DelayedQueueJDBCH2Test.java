package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCH2Test extends DelayedQueueJDBCContractTestBase {

    private String createDbUrl() {
        return "jdbc:h2:mem:delayedqueue_h2_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
    }

    @Override
    protected DelayedQueueJDBC<String> createQueue() throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createDbUrl(),
            JdbcDriver.H2,
            "sa",
            "",
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "h2-test-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }

    @Override
    protected DelayedQueueJDBC<String> createQueueWithClock(MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createDbUrl(),
            JdbcDriver.H2,
            "sa",
            "",
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "h2-test-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }

    @Override
    protected DelayedQueueJDBC<String> createQueueWithClock(
        MutableClock clock,
        DelayedQueueTimeConfig timeConfig
    ) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createDbUrl(),
            JdbcDriver.H2,
            "sa",
            "",
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(dbConfig, "delayed_queue_test", timeConfig, "h2-test-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }
}
