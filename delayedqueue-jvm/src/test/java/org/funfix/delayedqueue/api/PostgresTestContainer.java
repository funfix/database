package org.funfix.delayedqueue.api;

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

final class PostgresTestContainer {
    private static final DockerImageName IMAGE =
        DockerImageName.parse("postgres:16-alpine");

    private static volatile PostgreSQLContainer<?> container;

    private PostgresTestContainer() {}

    static PostgreSQLContainer<?> container() {
        assumeDockerAvailable();
        if (container == null) {
            synchronized (PostgresTestContainer.class) {
                if (container == null) {
                    assumeDockerAvailable();
                    container =
                        new PostgreSQLContainer<>(IMAGE)
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
        Assumptions.assumeTrue(dockerAvailable, "Docker is not available; skipping PostgreSQL tests");
    }
}
