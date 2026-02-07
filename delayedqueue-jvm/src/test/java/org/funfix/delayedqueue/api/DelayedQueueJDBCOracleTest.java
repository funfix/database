package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCOracleTest extends DelayedQueueJDBCContractTestBase {
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

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "jdbc-oracle-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }

    @Override
    protected DelayedQueueJDBC<String> createQueueWithClock(MutableClock clock) throws Exception {
        var container = OracleTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.Oracle,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "jdbc-oracle-queue");

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
        var container = OracleTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.Oracle,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(dbConfig, "delayed_queue_test", timeConfig, "jdbc-oracle-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }
}
