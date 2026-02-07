package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCH2AdvancedTest extends DelayedQueueJDBCAdvancedTestBase {

    private String sharedDbUrl;

    private String createDbUrl() {
        return "jdbc:h2:mem:delayedqueue_h2_advanced_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
    }

    @Override
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createDbUrl(),
            JdbcDriver.H2,
            "sa",
            "",
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "h2-advanced-test-queue"
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
            JdbcDriver.H2,
            "sa",
            "",
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "h2-shared-db-test-queue-" + tableName
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
