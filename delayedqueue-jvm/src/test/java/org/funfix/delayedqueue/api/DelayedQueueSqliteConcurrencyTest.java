package org.funfix.delayedqueue.api;

import java.io.File;
import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueSqliteConcurrencyTest extends DelayedQueueJDBCConcurrencyTestBase {

    private String createTempDbUrl() throws Exception {
        File tempDb = File.createTempFile("delayedqueue_concurrency_", ".db");
        tempDb.deleteOnExit();
        return "jdbc:sqlite:" + tempDb.getAbsolutePath();
    }

    @Override
    protected DelayedQueueJDBC<String> createQueue() throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createTempDbUrl(),
            JdbcDriver.Sqlite,
            null,
            null,
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "sqlite-concurrency-test-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }
}
