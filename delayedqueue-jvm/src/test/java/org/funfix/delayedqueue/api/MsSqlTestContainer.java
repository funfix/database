package org.funfix.delayedqueue.api;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

final class MsSqlTestContainer {
    private static final DockerImageName IMAGE =
        DockerImageName.parse("mcr.microsoft.com/azure-sql-edge:1.0.7")
            .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");

    private static volatile MSSQLServerContainer<?> container;

    private MsSqlTestContainer() {}

    static MSSQLServerContainer<?> container() {
        if (container == null) {
            synchronized (MsSqlTestContainer.class) {
                if (container == null) {
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
}
