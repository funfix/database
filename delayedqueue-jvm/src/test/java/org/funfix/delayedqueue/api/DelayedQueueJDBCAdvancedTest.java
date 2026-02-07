package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueJDBCAdvancedTest extends DelayedQueueJDBCAdvancedTestBase {

    @Override
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            "jdbc:hsqldb:mem:testdb_advanced_" + System.currentTimeMillis(),
            JdbcDriver.HSQLDB,
            "SA",
            "",
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT_TESTING,
            "advanced-test-queue"
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
            "shared-db-test-queue-" + tableName
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
        return "jdbc:hsqldb:mem:shared_db_" + System.currentTimeMillis();
    }
}
