package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
public class DelayedQueueJDBCMariaDbAdvancedTest extends DelayedQueueJDBCAdvancedTestBase {
    @Override
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var container = MariaDbTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.MariaDB,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT_TESTING,
            "advanced-mariadb-test-queue"
        );

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }

    @Override
    protected DelayedQueueJDBC<String> createQueueOnSameDB(String url, String tableName, MutableClock clock) throws Exception {
        var container = MariaDbTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.MariaDB,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT_TESTING,
            "shared-mariadb-test-queue-" + tableName
        );

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }

    @Override
    protected String databaseUrl() {
        return MariaDbTestContainer.container().getJdbcUrl();
    }
}
