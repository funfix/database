package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters

/**
 * MariaDB-specific exception filters.
 *
 * MariaDB shares the same error codes as MySQL, so we use the shared MySQLCompatibleFilters.
 */
internal object MariaDBFilters : RdbmsExceptionFilters by MySQLCompatibleFilters
