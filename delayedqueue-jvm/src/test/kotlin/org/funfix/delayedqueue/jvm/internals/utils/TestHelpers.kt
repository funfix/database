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

package org.funfix.delayedqueue.jvm.internals.utils

import java.sql.SQLException

/**
 * Test helper for running database tests that may throw InterruptedException or SQLException.
 *
 * In tests, these exceptions can be thrown directly without special handling.
 */
@Throws(InterruptedException::class, SQLException::class)
internal fun <T> sneakyRunDB(block: () -> T): T = block()
