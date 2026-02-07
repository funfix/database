package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCOracleConcurrencyTest extends DelayedQueueJDBCConcurrencyTestBase {
    @Override
    protected DelayedQueueJDBC<String> createQueue() throws Exception {
        var container = OracleTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.Oracle,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "oracle-concurrency-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }
}
