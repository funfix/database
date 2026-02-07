package org.funfix.delayedqueue.jvm.internals.jdbc.mysql

import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.mariadb.MySQLCompatibleFilters

/**
 * MySQL-specific exception filters.
 *
 * MySQL shares the same error codes as MariaDB, so we use the shared MySQLCompatibleFilters.
 */
internal object MySQLFilters : RdbmsExceptionFilters by MySQLCompatibleFilters
