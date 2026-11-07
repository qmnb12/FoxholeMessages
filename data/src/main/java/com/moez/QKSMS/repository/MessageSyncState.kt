package dev.octoshrimpy.quik.repository

import dev.octoshrimpy.quik.model.Message

/**
 * State that must survive rebuilding Message objects from Android's SMS/MMS provider.
 *
 * Read/seen flags are mirrored to the provider asynchronously so UI state can update
 * immediately. Preserving the committed Realm values prevents an overlapping full or
 * incremental sync from restoring stale provider flags during that short window.
 */
internal data class MessageSyncState(
    val read: Boolean,
    val seen: Boolean,
    val deletedAt: Long?,
    val category: String?,
    val isOtp: Boolean,
    val isStarred: Boolean
) {
    fun applyTo(message: Message) {
        message.read = read
        message.seen = seen
        message.deletedAt = deletedAt
        message.category = category
        message.isOtp = isOtp
        message.isStarred = isStarred
    }

    companion object {
        fun from(message: Message) = MessageSyncState(
            read = message.read,
            seen = message.seen,
            deletedAt = message.deletedAt,
            category = message.category,
            isOtp = message.isOtp,
            isStarred = message.isStarred
        )
    }
}
