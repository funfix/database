package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
public class DelayedQueueJDBCMsSqlAdvancedTest extends DelayedQueueJDBCAdvancedTestBase {
    @Override
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var container = MsSqlTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.MsSqlServer,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT_TESTING,
            "advanced-mssql-test-queue"
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
        var container = MsSqlTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.MsSqlServer,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT_TESTING,
            "shared-mssql-test-queue-" + tableName
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
        return MsSqlTestContainer.container().getJdbcUrl();
    }
}
