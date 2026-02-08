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

package org.funfix.delayedqueue.jvm

/**
 * Runtime exception thrown in case of exceptions happening that are not recoverable, rendering
 * DelayedQueue inaccessible.
 *
 * This exception can be thrown by all DelayedQueue operations when:
 * - Issues with the RDBMS occur (bugs, or connection unavailable, failing after multiple retries)
 * - Database operations fail after exhausting all configured retry attempts
 * - Unrecoverable errors prevent the queue from functioning properly
 *
 * Example scenarios:
 * - Database connection pool exhausted
 * - Database server unreachable
 * - Transaction deadlocks that persist after retries
 * - Database schema inconsistencies
 */
public class ResourceUnavailableException(message: String?, cause: Throwable?) :
    RuntimeException(message, cause)
