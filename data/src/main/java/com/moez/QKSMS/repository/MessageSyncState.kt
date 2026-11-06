package dev.octoshrimpy.quik.repository

import dev.octoshrimpy.quik.model.Message

/**
 * State owned by Foxhole rather than Android's SMS/MMS provider.
 *
 * A full sync recreates Message objects from provider cursors, so this state must be carried
 * across that rebuild by (type, contentId). It is also reused by incremental sync.
 */
internal data class MessageSyncState(
    val deletedAt: Long?,
    val category: String?,
    val isOtp: Boolean,
    val isStarred: Boolean
) {
    fun applyTo(message: Message) {
        message.deletedAt = deletedAt
        message.category = category
        message.isOtp = isOtp
        message.isStarred = isStarred
    }

    companion object {
        fun from(message: Message) = MessageSyncState(
            deletedAt = message.deletedAt,
            category = message.category,
            isOtp = message.isOtp,
            isStarred = message.isStarred
        )
    }
}
