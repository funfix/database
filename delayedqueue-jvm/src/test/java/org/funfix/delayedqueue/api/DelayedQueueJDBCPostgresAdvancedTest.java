package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
public class DelayedQueueJDBCPostgresAdvancedTest extends DelayedQueueJDBCAdvancedTestBase {
    @Override
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var container = PostgresTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            container.getJdbcUrl(),
            JdbcDriver.PostgreSQL,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "advanced-postgres-test-queue"
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
        var container = PostgresTestContainer.container();
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.PostgreSQL,
            container.getUsername(),
            container.getPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "shared-postgres-test-queue-" + tableName
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
        return PostgresTestContainer.container().getJdbcUrl();
    }
}
