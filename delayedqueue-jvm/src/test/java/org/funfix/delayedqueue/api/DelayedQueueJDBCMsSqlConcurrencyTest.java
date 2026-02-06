package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DelayedQueueJDBCMsSqlConcurrencyTest extends DelayedQueueJDBCConcurrencyTestBase {
    private String databaseName;

    @BeforeAll
    public void createDatabase() throws Exception {
        databaseName = MsSqlLocalDatabase.createDatabase();
    }

    @AfterAll
    public void dropDatabase() throws Exception {
        if (databaseName != null) {
            MsSqlLocalDatabase.dropDatabase(databaseName);
        }
    }

    @Override
    protected DelayedQueueJDBC<String> createQueue() throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            MsSqlLocalDatabase.jdbcUrl(databaseName),
            JdbcDriver.MsSqlServer,
            "sa",
            MsSqlLocalDatabase.adminPassword(),
            null
        );

        var queueConfig = DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue_test", "concurrency-mssql-queue");

        DelayedQueueJDBC.runMigrations(queueConfig);

        return DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        );
    }
}
