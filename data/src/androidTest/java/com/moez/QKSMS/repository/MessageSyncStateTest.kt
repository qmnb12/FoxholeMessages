package dev.octoshrimpy.quik.repository

import androidx.test.runner.AndroidJUnit4
import dev.octoshrimpy.quik.model.Message
import org.junit.Assert.assertEquals
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
            deletedAt = 123456L
            category = "TRANSACTIONAL"
            isOtp = true
            isStarred = true
        }
        val providerMessage = Message().apply {
            type = Message.TYPE_SMS
            contentId = 42L
        }

        MessageSyncState.from(existing).applyTo(providerMessage)

        assertEquals(123456L, providerMessage.deletedAt)
        assertEquals("TRANSACTIONAL", providerMessage.category)
        assertTrue(providerMessage.isOtp)
        assertTrue(providerMessage.isStarred)
    }
}
