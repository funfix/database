package org.funfix.delayedqueue.api;

import java.io.File;
import org.funfix.delayedqueue.jvm.*;

public class DelayedQueueSqliteTest extends DelayedQueueJDBCContractTestBase {

    private File tempDb;

    private String createTempDbUrl() throws Exception {
        tempDb = File.createTempFile("delayedqueue_test_", ".db");
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

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "sqlite-test-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }

    @Override
    protected DelayedQueueJDBC<String> createQueueWithClock(MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            createTempDbUrl(),
            JdbcDriver.Sqlite,
            null,
            null,
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "sqlite-test-queue");

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
        var dbConfig = new JdbcConnectionConfig(
            createTempDbUrl(),
            JdbcDriver.Sqlite,
            null,
            null,
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(dbConfig, "delayed_queue_test", timeConfig, "sqlite-test-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig,
            clock
        );
    }
}
