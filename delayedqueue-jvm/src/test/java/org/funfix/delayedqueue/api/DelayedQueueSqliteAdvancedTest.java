/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            DelayedQueueTimeConfig.DEFAULT_TESTING,
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
            DelayedQueueTimeConfig.DEFAULT_TESTING,
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
