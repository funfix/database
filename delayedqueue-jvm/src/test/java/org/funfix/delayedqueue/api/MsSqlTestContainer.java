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
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

final class MsSqlTestContainer {
    private static final DockerImageName IMAGE =
        DockerImageName.parse("mcr.microsoft.com/azure-sql-edge:1.0.7")
            .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");

    private static volatile MSSQLServerContainer container;

    private MsSqlTestContainer() {}

    static MSSQLServerContainer container() {
        assumeDockerAvailable();
        if (container == null) {
            synchronized (MsSqlTestContainer.class) {
                if (container == null) {
                    assumeDockerAvailable();
                    container =
                        new MSSQLServerContainer(IMAGE)
                            .acceptLicense()
                            .withPassword("StrongPassword!123");
                    container.start();
                }
            }
        }
        return container;
    }

    private static void assumeDockerAvailable() {
        boolean dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        Assumptions.assumeTrue(dockerAvailable, "Docker is not available; skipping MS-SQL tests");
    }
}
