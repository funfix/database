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
    val payload: String,
    val scheduledAt: Instant,
    val scheduledAtInitially: Instant,
    val lockUuid: String?,
    val createdAt: Instant,
) {
    /**
     * Checks if this row is a duplicate of another (same key, payload, and initial schedule).
     * Used to detect idempotent updates.
     */
    fun isDuplicate(other: DBTableRow): Boolean =
        pKey == other.pKey &&
            pKind == other.pKind &&
            payload == other.payload &&
            scheduledAtInitially == other.scheduledAtInitially
}

/**
 * Database table row with auto-generated ID.
 *
 * @property id Auto-generated row ID from database
 * @property data The actual row data
 */
internal data class DBTableRowWithId(
    val id: Long,
    val data: DBTableRow,
)
