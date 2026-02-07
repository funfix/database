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

package org.funfix.delayedqueue.jvm.internals.jdbc

import java.time.Instant

/**
 * Internal representation of a row in the delayed queue database table.
 *
 * @property pKey Unique message key within a kind
 * @property pKind Message kind/partition (MD5 hash of the queue type)
 * @property payload Serialized message payload
 * @property scheduledAt When the message should be delivered
 * @property scheduledAtInitially Original scheduled time (for debugging)
 * @property lockUuid Lock identifier when message is being processed
 * @property createdAt Timestamp when row was created
 */
internal data class DBTableRow(
    val pKey: String,
    val pKind: String,
    val payload: ByteArray,
    val scheduledAt: Instant,
    val scheduledAtInitially: Instant,
    val lockUuid: String?,
    val createdAt: Instant,
) {
    /**
     * Checks if this row is a duplicate of another (same key, payload, and initial schedule). Used
     * to detect idempotent updates.
     */
    fun isDuplicate(other: DBTableRow): Boolean =
        pKey == other.pKey &&
            pKind == other.pKind &&
            payload.contentEquals(other.payload) &&
            scheduledAtInitially == other.scheduledAtInitially
}

/**
 * Database table row with auto-generated ID.
 *
 * @property id Auto-generated row ID from database
 * @property data The actual row data
 */
internal data class DBTableRowWithId(val id: Long, val data: DBTableRow)
