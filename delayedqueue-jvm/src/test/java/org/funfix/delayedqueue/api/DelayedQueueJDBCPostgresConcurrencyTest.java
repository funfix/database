package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
public class DelayedQueueJDBCPostgresConcurrencyTest extends DelayedQueueJDBCConcurrencyTestBase {
    @Override
    protected DelayedQueueJDBC<String> createQueue() throws Exception {
        var container = PostgresTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.PostgreSQL,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "concurrency-postgres-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }
}
