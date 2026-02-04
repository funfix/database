package org.funfix.delayedqueue.jvm;

import java.util.Optional;

/**
 * Represents the configuration for a JDBC connection.
 */
public record JdbcConnectionConfig(
    String url,
    JdbcDriver driver,
    Optional<String> username,
    Optional<String> password,
    Optional<JdbcDatabasePoolConfig> pool
) {}
