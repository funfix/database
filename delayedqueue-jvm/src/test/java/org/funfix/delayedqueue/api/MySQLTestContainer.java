package org.funfix.delayedqueue.api;

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mysql.MySQLContainer;

final class MySQLTestContainer {
    private static final String IMAGE = "mysql:9.3";

    private static volatile MySQLContainer container;

    private MySQLTestContainer() {}

    static MySQLContainer container() {
        assumeDockerAvailable();
        if (container == null) {
            synchronized (MySQLTestContainer.class) {
                if (container == null) {
                    assumeDockerAvailable();
                    MySQLContainer localContainer =
                        new MySQLContainer(IMAGE)
                            .withDatabaseName("testdb")
                            .withUsername("test")
                            .withPassword("test")
                            .withCommand("--max-connections=500");
                    localContainer.start();
                    container = localContainer;
                }
            }
        }
        return container;
    }

    private static void assumeDockerAvailable() {
        boolean dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        Assumptions.assumeTrue(dockerAvailable, "Docker is not available; skipping MySQL tests");
    }
}
