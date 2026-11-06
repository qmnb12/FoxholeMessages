package dev.octoshrimpy.quik.repository

/**
 * Builds a bounded selection for updating one provider flag across a batch of SMS/MMS threads.
 * When [value] is true, only rows currently false are selected; when false, only true rows
 * are selected. The caller chunks [threadIds] before calling this helper.
 */
internal object TelephonyMessageSelection {

    fun forThreadFlag(
        flagColumn: String,
        threadIdColumn: String,
        value: Boolean,
        threadIds: Collection<Long>
    ): String {
        require(flagColumn == "read" || flagColumn == "seen") {
            "Unsupported telephony message flag: $flagColumn"
        }
        require(threadIds.isNotEmpty()) { "At least one thread ID is required" }

        val currentValue = if (value) 0 else 1
        return "$flagColumn = $currentValue AND $threadIdColumn IN (${threadIds.joinToString(",")})"
    }
}
