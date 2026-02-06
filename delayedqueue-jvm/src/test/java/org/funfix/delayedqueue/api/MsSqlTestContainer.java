package org.funfix.delayedqueue.api;

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

final class MsSqlTestContainer {
    private static final DockerImageName IMAGE =
        DockerImageName.parse("mcr.microsoft.com/azure-sql-edge:1.0.7")
            .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");

    private static volatile MSSQLServerContainer<?> container;

    private MsSqlTestContainer() {}

    static MSSQLServerContainer<?> container() {
        assumeDockerAvailable();
        if (container == null) {
            synchronized (MsSqlTestContainer.class) {
                if (container == null) {
                    assumeDockerAvailable();
                    container =
                        new MSSQLServerContainer<>(IMAGE)
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
