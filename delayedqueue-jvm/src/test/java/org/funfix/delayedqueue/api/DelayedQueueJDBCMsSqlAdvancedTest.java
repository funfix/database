package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DelayedQueueJDBCMsSqlAdvancedTest extends DelayedQueueJDBCAdvancedTestBase {
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
    protected DelayedQueueJDBC<String> createQueue(String tableName, MutableClock clock) throws Exception {
        var dbConfig = new JdbcConnectionConfig(
            MsSqlLocalDatabase.jdbcUrl(databaseName),
            JdbcDriver.MsSqlServer,
            "sa",
            MsSqlLocalDatabase.adminPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
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
        var dbConfig = new JdbcConnectionConfig(
            url,
            JdbcDriver.MsSqlServer,
            "sa",
            MsSqlLocalDatabase.adminPassword(),
            null
        );

        var queueConfig = new DelayedQueueJDBCConfig(
            dbConfig,
            tableName,
            DelayedQueueTimeConfig.DEFAULT,
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
        return MsSqlLocalDatabase.jdbcUrl(databaseName);
    }
}
