package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCH2ConcurrencyTest extends DelayedQueueJDBCConcurrencyTestBase {

    private String createDbUrl() {
        return "jdbc:h2:mem:delayedqueue_h2_concurrency_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
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

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "h2-concurrency-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }
}
