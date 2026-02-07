/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters

/**
 * MariaDB-specific exception filters.
 *
 * MariaDB shares the same error codes as MySQL, so we use the shared MySQLCompatibleFilters.
 */
internal object MariaDBFilters : RdbmsExceptionFilters by MySQLCompatibleFilters
