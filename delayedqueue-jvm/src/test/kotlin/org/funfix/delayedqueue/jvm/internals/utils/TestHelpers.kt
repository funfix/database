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
 * Test helper to call database APIs that declare context receivers for checked exceptions. It
 * supplies unsafe Raise contexts for InterruptedException and SQLException so tests can call
 * internal methods without changing production code.
 */
internal fun <T> sneakyRunDB(
    block:
        context(Raise<InterruptedException>, Raise<SQLException>)
        () -> T
): T = block(Raise._PRIVATE_AND_UNSAFE, Raise._PRIVATE_AND_UNSAFE)
