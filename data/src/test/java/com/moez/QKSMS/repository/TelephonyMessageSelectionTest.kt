package dev.octoshrimpy.quik.repository

import org.junit.Assert.assertEquals
import org.junit.Test

class TelephonyMessageSelectionTest {

    @Test
    fun markingReadTrue_selectsOnlyUnreadRowsInRequestedThreads() {
        assertEquals(
            "read = 0 AND thread_id IN (12,34)",
            TelephonyMessageSelection.forThreadFlag("read", "thread_id", true, listOf(12L, 34L))
        )
    }

    @Test
    fun markingReadFalse_selectsOnlyReadRowsInRequestedThreads() {
        assertEquals(
            "read = 1 AND thread_id IN (56)",
            TelephonyMessageSelection.forThreadFlag("read", "thread_id", false, listOf(56L))
        )
    }

    @Test
    fun markingSeenTrue_selectsOnlyUnseenRowsInRequestedThreads() {
        assertEquals(
            "seen = 0 AND thread_id IN (7,8)",
            TelephonyMessageSelection.forThreadFlag("seen", "thread_id", true, listOf(7L, 8L))
        )
    }

    @Test(expected = IllegalArgumentException::class)
    fun emptyThreadBatch_isRejected() {
        TelephonyMessageSelection.forThreadFlag("read", "thread_id", true, emptyList())
    }
}
