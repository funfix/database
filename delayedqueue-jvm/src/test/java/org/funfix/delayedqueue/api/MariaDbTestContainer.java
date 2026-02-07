package org.funfix.delayedqueue.api;

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MariaDBContainer;

final class MariaDbTestContainer {
    private static final String IMAGE = "mariadb:11.7";

    private static volatile MariaDBContainer<?> container;

    private MariaDbTestContainer() {}

    static MariaDBContainer<?> container() {
        assumeDockerAvailable();
        if (container == null) {
            synchronized (MariaDbTestContainer.class) {
                if (container == null) {
                    assumeDockerAvailable();
                    container =
                        new MariaDBContainer<>(IMAGE)
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
