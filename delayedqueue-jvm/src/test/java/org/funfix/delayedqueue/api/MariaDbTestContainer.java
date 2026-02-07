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

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mariadb.MariaDBContainer;

final class MariaDbTestContainer {
    private static final String IMAGE = "mariadb:11.7";

    private static volatile MariaDBContainer container;

    private MariaDbTestContainer() {}

    static MariaDBContainer container() {
        assumeDockerAvailable();
        if (container == null) {
            synchronized (MariaDbTestContainer.class) {
                if (container == null) {
                    assumeDockerAvailable();
                    container =
                        new MariaDBContainer(IMAGE)
                            .withDatabaseName("testdb")
                            .withUsername("test")
                            .withPassword("test");
                    container.start();
                }
            }
        }
        return container;
    }

    private static void assumeDockerAvailable() {
        boolean dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        Assumptions.assumeTrue(dockerAvailable, "Docker is not available; skipping MariaDB tests");
    }
}
