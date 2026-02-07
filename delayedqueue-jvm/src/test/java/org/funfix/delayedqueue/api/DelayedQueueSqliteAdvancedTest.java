package org.funfix.delayedqueue.api;

import java.io.File;
import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueSqliteAdvancedTest extends DelayedQueueJDBCAdvancedTestBase {

    private String sharedDbUrl;

    private String createTempDbUrl() throws Exception {
        File tempDb = File.createTempFile("delayedqueue_advanced_", ".db");
        tempDb.deleteOnExit();
        return "jdbc:sqlite:" + tempDb.getAbsolutePath();
    }

    @Override
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createTempDbUrl(),
            JdbcDriver.Sqlite,
            null,
            null,
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "sqlite-advanced-test-queue"
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
            JdbcDriver.Sqlite,
            null,
            null,
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
            "sqlite-shared-db-test-queue-" + tableName
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
        try {
            if (sharedDbUrl == null) {
                sharedDbUrl = createTempDbUrl();
            }
            return sharedDbUrl;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
