package dev.octoshrimpy.quik.repository

import androidx.test.runner.AndroidJUnit4
import dev.octoshrimpy.quik.model.Message
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class MessageSyncStateTest {

    @Test
    fun fullSyncRebuild_preservesFoxholeMessageState() {
        val existing = Message().apply {
            type = Message.TYPE_SMS
            contentId = 42L
            read = true
            seen = true
            deletedAt = 123456L
            category = "TRANSACTIONAL"
            isOtp = true
            isStarred = true
        }
        val providerMessage = Message().apply {
            type = Message.TYPE_SMS
            contentId = 42L
            read = false
            seen = false
        }

        MessageSyncState.from(existing).applyTo(providerMessage)

        assertTrue(providerMessage.read)
        assertTrue(providerMessage.seen)
        assertEquals(123456L, providerMessage.deletedAt)
        assertEquals("TRANSACTIONAL", providerMessage.category)
        assertTrue(providerMessage.isOtp)
        assertTrue(providerMessage.isStarred)
    }

    @Test
    fun fullSyncRebuild_preservesUnreadStateToo() {
        val existing = Message().apply {
            type = Message.TYPE_SMS
            contentId = 43L
            read = false
            seen = false
        }
        val providerMessage = Message().apply {
            type = Message.TYPE_SMS
            contentId = 43L
            read = true
            seen = true
        }

        MessageSyncState.from(existing).applyTo(providerMessage)

        assertFalse(providerMessage.read)
        assertFalse(providerMessage.seen)
    }
}
