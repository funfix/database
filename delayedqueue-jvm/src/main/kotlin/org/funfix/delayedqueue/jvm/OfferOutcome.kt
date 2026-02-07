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
 * Outcome of offering a message to the delayed queue.
 *
 * This sealed interface represents the possible results when adding or updating a message in the
 * queue.
 */
public sealed interface OfferOutcome {
    /** Returns true if the offer was ignored (message already exists and cannot be updated). */
    public fun isIgnored(): Boolean = this is Ignored

    /** Message was successfully created (new entry). */
    public data object Created : OfferOutcome

    /** Message was successfully updated (existing entry modified). */
    public data object Updated : OfferOutcome

    /** Message offer was ignored (already exists and canUpdate was false). */
    public data object Ignored : OfferOutcome
}
