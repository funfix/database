package org.funfix.delayedqueue.jvm;

import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for tuning the Hikari pool connection.
 */
public record JdbcDatabasePoolConfig(
    Duration connectionTimeout,
    Duration idleTimeout,
    Duration maxLifetime,
    Duration keepaliveTime,
    int maximumPoolSize,
    Optional<Integer> minimumIdle,
    Optional<Duration> leakDetectionThreshold,
    Optional<Duration> initializationFailTimeout
) {}
