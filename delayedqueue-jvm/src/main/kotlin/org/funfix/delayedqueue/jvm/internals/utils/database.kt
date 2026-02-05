package org.funfix.delayedqueue.jvm.internals.utils

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.ExecutionException
import javax.sql.DataSource
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig
import org.funfix.tasks.jvm.Task
import org.funfix.tasks.jvm.TaskCancellationException
import org.funfix.tasks.jvm.TaskExecutors
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(ConnectionPool::class.java)

internal class Database(val source: DataSource, private val closeRef: AutoCloseable) :
    AutoCloseable {
    override fun close() {
        closeRef.close()
    }

    companion object {
        operator fun invoke(config: JdbcConnectionConfig): Database {
            val ref = ConnectionPool.createDataSource(config)
            return Database(ref, ref)
        }
    }
}

@JvmInline internal value class SafeConnection(val underlying: Connection)

context(_: Raise<SQLException>)
internal inline fun <T> runSQLOperation(block: () -> T): T = block()

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <T> Database.withConnection(block: (SafeConnection) -> T): T = runBlockingIO {
    runSQLOperation {
        source.connection.let {
            try {
                block(SafeConnection(it))
            } finally {
                try {
                    it.close()
                } catch (e: SQLException) {
                    logger.warn("While closing JDBC connection", e)
                }
            }
        }
    }
}

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <T> Database.withTransaction(block: (SafeConnection) -> T) =
    withConnection { connection ->
        val autoCommit = connection.underlying.autoCommit
        try {
            connection.underlying.autoCommit = false
            val result = block(connection)
            connection.underlying.commit()
            result
        } catch (e: Exception) {
            try {
                connection.underlying.rollback()
            } catch (rollbackEx: SQLException) {
                e.addSuppressed(rollbackEx)
            }
            throw e
        } finally {
            connection.underlying.autoCommit = autoCommit
        }
    }

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <T> SafeConnection.execute(sql: String): Boolean =
    withStatement({ it.createStatement() }) { statement -> statement.execute(sql) }

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <T> SafeConnection.query(sql: String, block: (ResultSet) -> T): T =
    withStatement({ it.prepareStatement(sql) }) { statement ->
        statement.executeQuery().use { resultSet -> block(resultSet) }
    }

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <Stm : Statement, T> SafeConnection.withStatement(
    createStatement: (Connection) -> Stm,
    block: (Stm) -> T,
): T =
    createStatement(underlying).use { statement ->
        val executor = TaskExecutors.sharedBlockingIO()
        val task =
            Task.fromBlockingIO { block(statement) }
                .ensureRunningOnExecutor(executor)
                .withCancellation {
                    executor.execute {
                        try {
                            statement.cancel()
                        } catch (e: SQLException) {
                            logger.warn("While closing JDBC statement", e)
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

internal object ConnectionPool {
    fun buildHikariConfig(config: JdbcConnectionConfig): HikariConfig {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = config.url
        hikariConfig.driverClassName = config.driver.className
        config.username?.let { hikariConfig.username = it }
        config.password?.let { hikariConfig.password = it }

        config.pool?.let { pool ->
            hikariConfig.connectionTimeout = pool.connectionTimeout.toMillis()
            hikariConfig.idleTimeout = pool.idleTimeout.toMillis()
            hikariConfig.maxLifetime = pool.maxLifetime.toMillis()
            hikariConfig.keepaliveTime = pool.keepaliveTime.toMillis()
            hikariConfig.maximumPoolSize = pool.maximumPoolSize
            pool.minimumIdle?.let { hikariConfig.minimumIdle = it }
            pool.leakDetectionThreshold?.let { hikariConfig.leakDetectionThreshold = it.toMillis() }
            pool.initializationFailTimeout?.let {
                hikariConfig.initializationFailTimeout = it.toMillis()
            }
        }

        return hikariConfig
    }

    fun createDataSource(config: JdbcConnectionConfig): HikariDataSource =
        HikariDataSource(buildHikariConfig(config))
}
