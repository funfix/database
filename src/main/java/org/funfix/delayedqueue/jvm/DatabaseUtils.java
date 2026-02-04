package org.funfix.delayedqueue.jvm;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.funfix.tasks.jvm.*;
import org.jetbrains.annotations.Nullable;
import org.jspecify.annotations.NullMarked;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

@FunctionalInterface
interface CreateStatementFun<T extends Statement> {
    T call(Connection connection) throws SQLException;
}

@NullMarked
final class DatabaseUtils {
    static HikariConfig buildHikariConfig(JdbcConnectionConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.url());
        hikariConfig.setDriverClassName(config.driver().className());
        config.username().ifPresent(hikariConfig::setUsername);
        config.password().ifPresent(hikariConfig::setPassword);

        config.pool().ifPresent(pool -> {
            hikariConfig.setConnectionTimeout(pool.connectionTimeout().toMillis());
            hikariConfig.setIdleTimeout(pool.idleTimeout().toMillis());
            hikariConfig.setMaxLifetime(pool.maxLifetime().toMillis());
            hikariConfig.setKeepaliveTime(pool.keepaliveTime().toMillis());
            hikariConfig.setMaximumPoolSize(pool.maximumPoolSize());
            pool.minimumIdle().ifPresent(hikariConfig::setMinimumIdle);
            pool.leakDetectionThreshold().ifPresent(
                duration -> hikariConfig.setLeakDetectionThreshold(duration.toMillis())
            );
            pool.initializationFailTimeout().ifPresent(
                duration -> hikariConfig.setInitializationFailTimeout(duration.toMillis())
            );
        });

        return hikariConfig;
    }

    static DataSource createDataSource(JdbcConnectionConfig config) {
        HikariConfig hikariConfig = buildHikariConfig(config);
        return new HikariDataSource(hikariConfig);
    }

    static <Stm extends Statement, T extends @Nullable Object> T runStatement(
        Connection connection,
        CreateStatementFun<Stm> createStatement,
        ProcessFun<Stm, T> process
    ) throws InterruptedException, SQLException {
        try (
            final var statement = createStatement.call(connection)
        ) {
            // Uses virtual threads when running on top of Java 21+
            final var executor = TaskExecutors.sharedBlockingIO();
            final var task = Task
                .fromBlockingIO(() -> process.call(statement))
                // Ensure that the task runs on virtual threads (Java 21+)
                .ensureRunningOnExecutor(executor)
                // Concurrent cancellation logic
                .withCancellation(() -> executor.execute(() -> {
                    try {
                        statement.cancel();
                    } catch (SQLException e) {
                        LoggerFactory.getLogger(DatabaseUtils.class).error(
                            "While closing JDBC statement", e);
                    }
                }));
            // Go, go, go!
            final var fiber = task.runFiber(TaskExecutors.sharedBlockingIO());
            try {
                return fiber.awaitBlocking();
            } catch (final InterruptedException e) {
                // Current thread was interrupted, so we are first cancelling the fiber
                fiber.cancel();
                // Then we wait on its termination
                fiber.joinBlockingUninterruptible();
                throw e;
            } catch (TaskCancellationException e) {
                // This exception type shouldn't happen
                throw new RuntimeException(e);
            } catch (final ExecutionException e) {
                // ExecutionException gets unwrapped because it wraps the
                // original exception that happened on another thread/fiber
                if (e.getCause() instanceof SQLException sqlEx) {
                    throw sqlEx;
                }
                if (e.getCause() instanceof RuntimeException runtimeEx) {
                    throw runtimeEx;
                }
                // Other exception types
                final var cause = e.getCause();
                throw new RuntimeException(cause != null ? cause : e);
            }
        }
    }
}
