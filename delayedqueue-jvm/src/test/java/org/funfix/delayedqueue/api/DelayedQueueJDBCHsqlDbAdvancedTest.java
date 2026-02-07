package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCHsqlDbAdvancedTest extends DelayedQueueJDBCAdvancedTestBase {

    private String sharedDbUrl;

    private String createDbUrl() {
        return "jdbc:hsqldb:mem:delayedqueue_hsqldb_advanced_" + System.nanoTime();
    }

    @Override
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createDbUrl(),
            JdbcDriver.HSQLDB,
            "SA",
            "",
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT_TESTING,
            "hsqldb-advanced-test-queue"
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
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.HSQLDB,
            "SA",
            "",
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT_TESTING,
            "hsqldb-shared-db-test-queue-" + tableName
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
        if (sharedDbUrl == null) {
            sharedDbUrl = createDbUrl();
        }
        return sharedDbUrl;
    }
}
