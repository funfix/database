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
