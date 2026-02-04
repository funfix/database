package org.funfix.delayedqueue.jvm

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.funfix.tasks.jvm.Task
import org.funfix.tasks.jvm.TaskCancellationException
import org.funfix.tasks.jvm.TaskExecutors
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.ExecutionException
import javax.sql.DataSource

@FunctionalInterface
internal fun interface CreateStatementFun<T : Statement> {
    @Throws(SQLException::class)
    fun call(connection: Connection): T
}

internal object DatabaseUtils {
    private val logger = LoggerFactory.getLogger(DatabaseUtils::class.java)

    fun buildHikariConfig(config: JdbcConnectionConfig): HikariConfig {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = config.url
        hikariConfig.driverClassName = config.driver.className
        config.username?.let { hikariConfig.username = it }
        config.password?.let { hikariConfig.password = it }

        config.pool?.let { pool ->
            hikariConfig.connectionTimeout = pool.connectionTimeout.inWholeMilliseconds
            hikariConfig.idleTimeout = pool.idleTimeout.inWholeMilliseconds
            hikariConfig.maxLifetime = pool.maxLifetime.inWholeMilliseconds
            hikariConfig.keepaliveTime = pool.keepaliveTime.inWholeMilliseconds
            hikariConfig.maximumPoolSize = pool.maximumPoolSize
            pool.minimumIdle?.let { hikariConfig.minimumIdle = it }
            pool.leakDetectionThreshold?.let {
                hikariConfig.leakDetectionThreshold = it.inWholeMilliseconds
            }
            pool.initializationFailTimeout?.let {
                hikariConfig.initializationFailTimeout = it.inWholeMilliseconds
            }
        }

        return hikariConfig
    }

    fun createDataSource(config: JdbcConnectionConfig): DataSource =
        HikariDataSource(buildHikariConfig(config))

    @Throws(InterruptedException::class, SQLException::class)
    @Suppress("TooGenericExceptionThrown", "SwallowedException", "ThrowsCount")
    fun <Stm : Statement, T> runStatement(
        connection: Connection,
        createStatement: CreateStatementFun<Stm>,
        process: (Stm) -> T
    ): T {
        createStatement.call(connection).use { statement ->
            val executor = TaskExecutors.sharedBlockingIO()
            val task = Task
                .fromBlockingIO { process(statement) }
                .ensureRunningOnExecutor(executor)
                .withCancellation {
                    executor.execute {
                        try {
                            statement.cancel()
                        } catch (e: SQLException) {
                            logger.error("While closing JDBC statement", e)
                        }
                    }
                }
            val fiber = task.runFiber(TaskExecutors.sharedBlockingIO())
            try {
                return fiber.awaitBlocking()
            } catch (e: InterruptedException) {
                fiber.cancel()
                fiber.joinBlockingUninterruptible()
                throw e
            } catch (e: TaskCancellationException) {
                throw RuntimeException(e)
            } catch (e: ExecutionException) {
                when (val cause = e.cause) {
                    is SQLException -> throw cause
                    is RuntimeException -> throw cause
                    else -> throw RuntimeException(cause ?: e)
                }
            }
        }
    }
}
