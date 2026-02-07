package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import org.funfix.delayedqueue.jvm.JdbcDriver

/**
 * MariaDB-specific adapter.
 *
 * MariaDB shares the same SQL syntax as MySQL, so we use the shared MySQLCompatibleAdapter.
 */
internal class MariaDBAdapter(driver: JdbcDriver, tableName: String) :
    MySQLCompatibleAdapter(driver, tableName)
