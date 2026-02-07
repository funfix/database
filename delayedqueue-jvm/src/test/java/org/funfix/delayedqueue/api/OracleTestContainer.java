package org.funfix.delayedqueue.api;

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

final class OracleTestContainer {
    private static final DockerImageName IMAGE =
        DockerImageName.parse("gvenzl/oracle-free:23.4-slim")
            .asCompatibleSubstituteFor("gvenzl/oracle-xe");

    private static volatile OracleContainer container;

    private OracleTestContainer() {}

    static OracleContainer container() {
        assumeDockerAvailable();
        if (container == null) {
            synchronized (OracleTestContainer.class) {
                if (container == null) {
                    assumeDockerAvailable();
                    OracleContainer newContainer =
                        new OracleContainer(IMAGE)
                            .withDatabaseName("testdb")
                            .withUsername("test")
                            .withPassword("test");
                    newContainer.start();
                    container = newContainer;
                }
            }
        }
        return container;
    }

    private static void assumeDockerAvailable() {
        boolean dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        Assumptions.assumeTrue(dockerAvailable, "Docker is not available; skipping Oracle tests");
    }
}
