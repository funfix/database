package org.funfix.delayedqueue.jvm.internals.jdbc.mysql

import org.funfix.delayedqueue.jvm.JdbcDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.mariadb.MySQLCompatibleAdapter

/**
 * MySQL-specific adapter.
 *
 * MySQL shares the same SQL syntax as MariaDB, so we use the shared MySQLCompatibleAdapter.
 */
internal class MySQLAdapter(driver: JdbcDriver, tableName: String) :
    MySQLCompatibleAdapter(driver, tableName)
