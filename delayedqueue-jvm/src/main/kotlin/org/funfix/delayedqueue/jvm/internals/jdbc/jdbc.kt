package org.funfix.delayedqueue.jvm.internals.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.ExecutionException
import javax.sql.DataSource
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig
import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.utils.Raise
import org.funfix.delayedqueue.jvm.internals.utils.runBlockingIO
import org.funfix.tasks.jvm.Task
import org.funfix.tasks.jvm.TaskCancellationException
import org.funfix.tasks.jvm.TaskExecutors
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(ConnectionPool::class.java)

internal class Database(
    val source: DataSource,
    val driver: JdbcDriver,
    private val closeRef: AutoCloseable,
) : AutoCloseable {
    override fun close() {
        closeRef.close()
    }

    companion object {
        operator fun invoke(config: JdbcConnectionConfig): Database {
            val ref = ConnectionPool.createDataSource(config)
            return Database(source = ref, closeRef = ref, driver = config.driver)
        }
    }
}

internal data class SafeConnection(val underlying: Connection, val driver: JdbcDriver)

context(_: Raise<SQLException>)
internal inline fun <T> runSQLOperation(block: () -> T): T = block()

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <T> Database.withConnection(block: (SafeConnection) -> T): T = runBlockingIO {
    runSQLOperation {
        source.connection.let {
            try {
                block(SafeConnection(it, driver))
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
internal fun SafeConnection.execute(sql: String): Boolean =
    withStatement({ it.createStatement() }) { statement -> statement.execute(sql) }

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <T> SafeConnection.createStatement(block: (Statement) -> T): T =
    withStatement({ it.createStatement() }, block)

context(_: Raise<InterruptedException>, _: Raise<SQLException>)
internal fun <T> SafeConnection.prepareStatement(sql: String, block: (PreparedStatement) -> T): T =
    withStatement({ it.prepareStatement(sql.trimIndent()) }, block)

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

        // SQLite-specific optimizations for concurrency
        if (config.driver == JdbcDriver.Sqlite) {
            hikariConfig.connectionInitSql =
                """
                PRAGMA journal_mode=WAL;
                PRAGMA busy_timeout=30000;
                PRAGMA synchronous=NORMAL;
                PRAGMA cache_size=-64000;
                PRAGMA temp_store=MEMORY;
                """
                    .trimIndent()
        }

        return hikariConfig
    }

    fun createDataSource(config: JdbcConnectionConfig): HikariDataSource =
        HikariDataSource(buildHikariConfig(config))
}

/**
 * Quotes a database identifier (table name, column name, etc.) with the appropriate quote syntax
 * for the target database system. This prevents naming conflicts and allows reserved keywords to be
 * used as identifiers.
 * - MariaDB: uses backticks (`)
 * - PostgreSQL, HSQLDB, H2, SQLite: use double quotes (")
 * - MS SQL Server: uses square brackets ([])
 *
 * @param name The identifier to quote
 * @return The quoted identifier in database-specific syntax
 */
internal fun JdbcDriver.quote(name: String): String =
    when (this) {
        JdbcDriver.MariaDB -> "`$name`"
        JdbcDriver.HSQLDB -> "\"$name\""
        JdbcDriver.H2 -> "\"$name\""
        JdbcDriver.PostgreSQL -> "\"$name\""
        JdbcDriver.Sqlite -> "\"$name\""
        JdbcDriver.MsSqlServer -> "[$name]"
        JdbcDriver.Oracle -> "\"$name\""
        else -> throw IllegalArgumentException("Unsupported JDBC driver: ${className}")
    }

/** Quotes a database identifier using the connection's driver. */
internal fun SafeConnection.quote(name: String): String = driver.quote(name)
