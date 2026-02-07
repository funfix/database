package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCHsqlDbConcurrencyTest extends DelayedQueueJDBCConcurrencyTestBase {

    private String createDbUrl() {
        return "jdbc:hsqldb:mem:delayedqueue_hsqldb_concurrency_" + System.nanoTime();
    }

    @Override
    protected DelayedQueueJDBC<String> createQueue() throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createDbUrl(),
            JdbcDriver.HSQLDB,
            "SA",
            "",
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "hsqldb-concurrency-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }
}
