package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
public class DelayedQueueJDBCMariaDbTest extends DelayedQueueJDBCContractTestBase {
    @Override
    protected DelayedQueueJDBC<String> createQueue() throws Exception {
        var container = MariaDbTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.MariaDB,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "jdbc-mariadb-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }

    @Override
    protected DelayedQueueJDBC<String> createQueueWithClock(MutableClock clock) throws Exception {
        var container = MariaDbTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.MariaDB,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "jdbc-mariadb-queue");

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
        var container = MariaDbTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.MariaDB,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(dbConfig, "delayed_queue_test", timeConfig, "jdbc-mariadb-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }
}
