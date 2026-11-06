/*
 * Copyright (C) 2017 Moez Bhatti <moez.bhatti@gmail.com>
 *
 * This file is part of QKSMS.
 *
 * QKSMS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * QKSMS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with QKSMS.  If not, see <http://www.gnu.org/licenses/>.
 */
package dev.octoshrimpy.quik.repository

import android.content.Context
import dev.octoshrimpy.quik.common.util.extensions.now
import dev.octoshrimpy.quik.compat.TelephonyCompat
import dev.octoshrimpy.quik.extensions.anyOf
import dev.octoshrimpy.quik.extensions.asObservable
import dev.octoshrimpy.quik.extensions.map
import dev.octoshrimpy.quik.filter.ConversationFilter
import dev.octoshrimpy.quik.mapper.CursorToConversation
import dev.octoshrimpy.quik.mapper.CursorToRecipient
import dev.octoshrimpy.quik.model.Contact
import dev.octoshrimpy.quik.model.Conversation
import dev.octoshrimpy.quik.model.Message
import dev.octoshrimpy.quik.model.Recipient
import dev.octoshrimpy.quik.model.SearchResult
import dev.octoshrimpy.quik.util.PhoneNumberUtils
import dev.octoshrimpy.quik.util.tryOrNull
import io.reactivex.Completable
import io.reactivex.Observable
import io.reactivex.android.schedulers.AndroidSchedulers
import io.reactivex.schedulers.Schedulers
import io.realm.Case
import io.realm.Realm
import io.realm.RealmQuery
import io.realm.RealmResults
import io.realm.Sort
import java.util.concurrent.TimeUnit
import javax.inject.Inject

class ConversationRepositoryImpl @Inject constructor(
    private val context: Context,
    private val conversationFilter: ConversationFilter,
    private val cursorToConversation: CursorToConversation,
    private val cursorToRecipient: CursorToRecipient,
    private val phoneNumberUtils: PhoneNumberUtils
) : ConversationRepository {
    private fun getConversationsBase(
        realm: Realm,
        unreadAtTop: Boolean,
        archived: Boolean
    ): RealmQuery<Conversation> {
        val sortOrder = mutableListOf("pinned", "draft", "lastMessage.date")
        val sortDirections = mutableListOf(Sort.DESCENDING, Sort.DESCENDING, Sort.DESCENDING)

        if (unreadAtTop) {
            sortOrder.add(0, "lastMessage.read")
            sortDirections.add(0, Sort.ASCENDING)
        }

        return realm
            .where(Conversation::class.java)
            .notEqualTo("id", 0L)
            .equalTo("archived", archived)
            .equalTo("blocked", false)
            .isNotEmpty("recipients")
            .beginGroup()
            .isNotNull("lastMessage")
            .or()
            .isNotEmpty("draft")
            .endGroup()
            .sort(sortOrder.toTypedArray(), sortDirections.toTypedArray())
    }

    override fun getConversations(
        unreadAtTop: Boolean,
        archived: Boolean
    ): RealmResults<Conversation> =
        getConversationsBase(Realm.getDefaultInstance(), unreadAtTop, archived)
            .findAllAsync()

    override fun getConversationsSnapshot(unreadAtTop: Boolean): List<Conversation> =
        Realm.getDefaultInstance().use { realm ->
            getConversationsBase(realm, unreadAtTop, false)
                .findAll()
                .let(realm::copyFromRealm)
        }

    override fun getTopConversations() =
        Realm.getDefaultInstance().use { realm ->
            val cutoff = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7)
            val conversations = realm.where(Conversation::class.java)
                .notEqualTo("id", 0L)
                .isNotNull("lastMessage")
                .beginGroup()
                .equalTo("pinned", true)
                .or()
                .greaterThan("lastMessage.date", cutoff)
                .endGroup()
                .equalTo("archived", false)
                .equalTo("blocked", false)
                .isNotEmpty("recipients")
                .findAll()
                .let(realm::copyFromRealm)

            // Avoid a Realm count query from inside the sort comparator. A comparator can run
            // many times per item, turning this into hundreds of database queries for a long list.
            val recentMessageCounts = realm.where(Message::class.java)
                .isNull("deletedAt")
                .greaterThan("date", cutoff)
                .findAll()
                .groupingBy { message -> message.threadId }
                .eachCount()

            conversations.sortedWith(
                compareByDescending<Conversation> { conversation -> conversation.pinned }
                    .thenByDescending { conversation -> recentMessageCounts[conversation.id] ?: 0 }
            )
        }

    override fun setConversationName(id: Long, name: String) =
        Completable.fromAction {
            Realm.getDefaultInstance().use { realm ->
                realm.executeTransaction {
                    realm.where(Conversation::class.java)
                        .equalTo("id", id)
                        .findFirst()
                        ?.name = name
                }
            }
        }.subscribeOn(Schedulers.io()) // Ensure the operation is performed on a background thread

    override fun searchConversations(query: CharSequence): List<SearchResult> {
        val realm = Realm.getDefaultInstance()
        val searchQuery = query.toString()

        return try {
            val conversations = realm.copyFromRealm(realm
                .where(Conversation::class.java)
                .notEqualTo("id", 0L)
                .isNotNull("lastMessage")
                .equalTo("blocked", false)
                .isNotEmpty("recipients")
                .sort("pinned", Sort.DESCENDING, "lastMessage.date", Sort.DESCENDING)
                .findAll())
            val conversationsById = conversations.associateBy { conversation -> conversation.id }

            val messagesByConversation = realm.copyFromRealm(realm
                .where(Message::class.java)
                .isNull("deletedAt")
                .beginGroup()
                .contains("body", searchQuery, Case.INSENSITIVE)
                .or()
                .contains("parts.text", searchQuery, Case.INSENSITIVE)
                .endGroup()
                .findAll())
                .groupBy { message -> message.threadId }
                .mapNotNull { (threadId, messages) ->
                    conversationsById[threadId]?.let { conversation ->
                        SearchResult(searchQuery, conversation, messages.size)
                    }
                }
                .sortedByDescending { result -> result.messages }

            conversations
                .filter { conversation -> conversationFilter.filter(conversation, searchQuery) }
                .map { conversation -> SearchResult(searchQuery, conversation, 0) } + messagesByConversation
        } finally {
            realm.close()
        }
    }

    override fun getBlockedConversations(): RealmResults<Conversation> =
        Realm.getDefaultInstance()
            .where(Conversation::class.java)
            .equalTo("blocked", true)
            .sort(
                arrayOf("lastMessage.date"),
                arrayOf(Sort.DESCENDING)
            )
            .findAll()

    override fun getBlockedConversationsAsync(): RealmResults<Conversation> =
        Realm.getDefaultInstance()
            .where(Conversation::class.java)
            .equalTo("blocked", true)
            .sort(
                arrayOf("lastMessage.date"),
                arrayOf(Sort.DESCENDING)
            )
            .findAllAsync()

    override fun getConversationAsync(threadId: Long): Conversation =
        Realm.getDefaultInstance()
            .where(Conversation::class.java)
            .equalTo("id", threadId)
            .findFirstAsync()

    override fun getConversation(threadId: Long) =
        tryOrNull(true) {
            Realm.getDefaultInstance()
                .apply { refresh() }
                .where(Conversation::class.java)
                .equalTo("id", threadId)
                .findFirst()
        }

    override fun updateSendAsGroup(threadId: Long, sendAsGroup: Boolean) =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            realm.where(Conversation::class.java)
                .equalTo("id", threadId)
                .findFirst()
                ?.let { conversation ->
                    realm.executeTransaction { conversation.sendAsGroup = sendAsGroup }
                }
        }

    // These APIs return an immediate snapshot, not a live RealmResults. Using findAllAsync()
    // here can return an empty, not-yet-loaded result when iterated immediately, silently
    // dropping notification IDs. Use a synchronous query and close its Realm afterward.
    override fun getUnseenIds(archived: Boolean): List<Long> =
        Realm.getDefaultInstance().use { realm ->
            realm.where(Conversation::class.java)
                .notEqualTo("id", 0L)
                .equalTo("archived", archived)
                .equalTo("blocked", false)
                .equalTo("lastMessage.seen", false)
                .sort(
                    arrayOf("lastMessage.date"),
                    arrayOf(Sort.DESCENDING)
                )
                .findAll()
                .map { conversation -> conversation.id }
        }

    override fun getUnreadIds(archived: Boolean): List<Long> =
        Realm.getDefaultInstance().use { realm ->
            realm.where(Conversation::class.java)
                .notEqualTo("id", 0L)
                .equalTo("archived", archived)
                .equalTo("blocked", false)
                .equalTo("lastMessage.read", false)
                .sort(
                    arrayOf("lastMessage.date"),
                    arrayOf(Sort.DESCENDING)
                )
                .findAll()
                .map { conversation -> conversation.id }
        }

    override fun getConversationAndLastSenderContactName(threadId: Long): Pair<Conversation?, String?>? =
        Realm.getDefaultInstance()
            .apply { refresh() }
            .where(Conversation::class.java)
            .equalTo("id", threadId)
            .findFirst()
            ?.let { conversation ->
                val conversationLastSmsSender: String? = conversation.lastMessage?.let { lastMessage ->
                    conversation.recipients.find { recipient ->
                        phoneNumberUtils.compare(recipient.address, lastMessage.address)
                    }?.contact?.name
                }

                Pair(conversation, conversationLastSmsSender)
            }

    override fun getConversations(vararg threadIds: Long): RealmResults<Conversation> =
        Realm.getDefaultInstance()
            .where(Conversation::class.java)
            .anyOf("id", threadIds)
            .findAll()

    override fun getUnmanagedConversations(): Observable<List<Conversation>> =
        Realm.getDefaultInstance().let { realm->
            realm.where(Conversation::class.java)
                .sort("lastMessage.date", Sort.DESCENDING)
                .notEqualTo("id", 0L)
                .isNotNull("lastMessage")
                .equalTo("archived", false)
                .equalTo("blocked", false)
                .isNotEmpty("recipients")
                .limit(5)
                .findAllAsync()
                .asObservable()
                .filter { it.isLoaded }
                .filter { it.isValid }
                .map { realm.copyFromRealm(it) }
                .subscribeOn(AndroidSchedulers.mainThread())
                .observeOn(Schedulers.io())
        }

    override fun getRecipients(): RealmResults<Recipient> =
        Realm.getDefaultInstance()
            .where(Recipient::class.java)
            .findAll()

    override fun getUnmanagedRecipients(): Observable<List<Recipient>> =
        Realm.getDefaultInstance().let { realm ->
            realm.where(Recipient::class.java)
                .isNotNull("contact")
                .findAllAsync()
                .asObservable()
                .filter { it.isLoaded && it.isValid }
                .map { realm.copyFromRealm(it) }
                .subscribeOn(AndroidSchedulers.mainThread())
        }

    override fun getRecipient(recipientId: Long): Recipient? =
        Realm.getDefaultInstance()
            .where(Recipient::class.java)
            .equalTo("id", recipientId)
            .findFirst()

    override fun createConversation(threadId: Long, sendAsGroup: Boolean) =
        createConversationFromCp(threadId, sendAsGroup)


    override fun getConversation(recipients: Collection<String>): Conversation? =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()
            realm.where(Conversation::class.java)
                .findAll()
                .filter { conversation -> conversation.recipients.size == recipients.size }
                .find { conversation ->
                    conversation.recipients.map { it.address }.all { recipientAddress ->
                        recipients.any { phoneNumberUtils.compare(it, recipientAddress) }
                    }
                }
                ?.let { realm.copyFromRealm(it) }
        }

    override fun createConversation(addresses: Collection<String>, sendAsGroup: Boolean) =
        TelephonyCompat.getOrCreateThreadId(context, addresses.toSet())
            .takeIf { it != 0L }
            ?.let { providerThreadId ->
                createConversationFromCp(providerThreadId, sendAsGroup) ?:
                    createEmptyConversation(providerThreadId, addresses, sendAsGroup)
            }

    override fun getOrCreateConversation(threadId: Long, sendAsGroup: Boolean) =
        getConversation(threadId) ?: createConversation(threadId, sendAsGroup)

    override fun getOrCreateConversation(addresses: Collection<String>, sendAsGroup: Boolean) =
        getConversation(addresses) ?: createConversation(addresses, sendAsGroup)

    override fun saveDraft(threadId: Long, draft: String) =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            val conversation = realm.where(Conversation::class.java)
                .equalTo("id", threadId)
                .findFirst()

            realm.executeTransaction {
                conversation?.takeIf { it.isValid }?.draft = draft
                conversation?.takeIf { it.isValid }?.draftDate = System.currentTimeMillis()
            }
        }

    override fun updateConversations(threadIds: Collection<Long>) =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            if (threadIds.isNotEmpty()) {
                val conversations = realm.where(Conversation::class.java)
                    .anyOf("id", threadIds.toLongArray())
                    .findAll()

                // Fetch all candidate messages in one query and keep the first (newest) message
                // per thread, instead of issuing one query for every changed conversation.
                val latestMessageByThreadId = HashMap<Long, Message>()
                realm.where(Message::class.java)
                    .anyOf("threadId", threadIds.toLongArray())
                    .isNull("deletedAt")
                    .sort("date", Sort.DESCENDING)
                    .findAll()
                    .forEach { message ->
                        if (!latestMessageByThreadId.containsKey(message.threadId)) {
                            latestMessageByThreadId[message.threadId] = message
                        }
                    }

                realm.executeTransaction {
                    conversations.forEach { conversation ->
                        conversation.lastMessage = latestMessageByThreadId[conversation.id]
                    }
                }
            }

            Unit
        }

    override fun markArchived(vararg threadIds: Long) =
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds)
                .findAll()

            realm.executeTransaction { conversations.forEach { it.archived = true } }
        }

    override fun markUnarchived(threadIds: Collection<Long>) =
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds.toLongArray())
                .findAll()

            realm.executeTransaction { conversations.forEach { it.archived = false } }
        }

    override fun markPinned(vararg threadIds: Long) =
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds)
                .findAll()

            realm.executeTransaction { conversations.forEach { it.pinned = true } }
        }

    override fun markUnpinned(vararg threadIds: Long) =
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds)
                .findAll()

            realm.executeTransaction { conversations.forEach { it.pinned = false } }
        }

    override fun markBlocked(threadIds: Collection<Long>, blockingClient: Int, blockReason: String?) =
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds.toLongArray())
                .equalTo("blocked", false)
                .findAll()

            realm.executeTransaction {
                conversations.forEach { conversation ->
                    conversation.blocked = true
                    conversation.blockingClient = blockingClient
                    conversation.blockReason = blockReason
                }
            }
        }

    override fun markUnblocked(vararg threadIds: Long) =
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds)
                .findAll()

            realm.executeTransaction {
                conversations.forEach { conversation ->
                    conversation.blocked = false
                    conversation.blockingClient = null
                    conversation.blockReason = null
                }
            }
        }

    // Trashes (soft-deletes) the conversation's messages rather than removing them outright -
    // matches how individual message deletion already works, so a deleted conversation is
    // recoverable from Trash within the retention window (see PurgeTrashService). The
    // conversation row itself is kept (not deleted) with lastMessage/draft cleared, which is
    // enough for getConversationsBase's isNotNull("lastMessage") OR isNotEmpty("draft") filter to
    // hide it from every list; restoring any of its messages naturally brings it back via
    // updateConversations() recomputing lastMessage.
    override fun deleteConversations(vararg threadIds: Long) {
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds)
                .findAll()
            val messages = realm.where(Message::class.java)
                .anyOf("threadId", threadIds)
                .isNull("deletedAt")
                .findAll()

            realm.executeTransaction {
                messages.forEach { message -> message.deletedAt = now() }
                conversations.forEach { conversation ->
                    conversation.lastMessage = null
                    conversation.draft = ""
                    conversation.draftDate = 0
                }
            }
        }
    }

    // Message sorting: conversations bucket into a Tab by their (possibly overridden) category.
    // A conversation with no override falls back to its last message's category, and PERSONAL
    // is the catch-all for anything that isn't TRANSACTIONAL or PROMOTIONAL (including
    // unclassified messages).
    private fun getConversationsByCategoryBase(
        realm: Realm,
        unreadAtTop: Boolean,
        category: String
    ): RealmQuery<Conversation> {
        val query = getConversationsBase(realm, unreadAtTop, false)
            .beginGroup()
            .beginGroup()
            .equalTo("categoryOverride", category)
            .or()
            .isNull("categoryOverride").equalTo("lastMessage.category", category)
            .endGroup()

        if (category == "PERSONAL") {
            query.or().beginGroup()
                .isNull("categoryOverride")
                .beginGroup()
                .isNull("lastMessage")
                .or()
                .not().`in`("lastMessage.category", arrayOf("TRANSACTIONAL", "PROMOTIONAL"))
                .endGroup()
                .endGroup()
        }

        return query.endGroup()
    }

    override fun getConversationsByCategory(unreadAtTop: Boolean, category: String): RealmResults<Conversation> =
        getConversationsByCategoryBase(Realm.getDefaultInstance(), unreadAtTop, category).findAllAsync()

    override fun getUnreadCountByCategory(category: String): Long =
        Realm.getDefaultInstance().use { realm ->
            getConversationsByCategoryBase(realm, false, category)
                .equalTo("lastMessage.read", false)
                .count()
        }

    override fun updateCategoryOverride(threadIds: Collection<Long>, category: String?) =
        Realm.getDefaultInstance().use { realm ->
            val conversations = realm.where(Conversation::class.java)
                .anyOf("id", threadIds.toLongArray())
                .findAll()

            realm.executeTransaction {
                conversations.forEach { conversation -> conversation.categoryOverride = category }
            }
        }

    /**
     * Returns a [Conversation] from the system SMS ContentProvider, based on the [threadId]
     *
     * It should be noted that even if we have a valid [threadId], that does not guarantee that
     * we can return a [Conversation]. On some devices, the ContentProvider won't return the
     * conversation unless it contains at least 1 message
     */
    private fun createConversationFromCp(threadId: Long, sendAsGroup: Boolean) =
        tryOrNull(true) {
            cursorToConversation.getConversationsCursor()
                ?.use { cursor -> cursor.map(cursorToConversation::map) }
                ?.firstOrNull { conversation -> conversation.id == threadId }
                ?.also { conversation ->
                    Realm.getDefaultInstance().use { realm ->
                        val realmContacts = realm.where(Contact::class.java).findAll()
                        val contactMatcher = phoneNumberUtils.contactMatcher(realmContacts)

                        // match recipients from provider to recipients in realm
                        val matchedRecipients = conversation.recipients
                            .mapNotNull { recipient ->
                                // map the recipient cursor to a list of recipients
                                cursorToRecipient.getRecipientCursor(recipient.id)?.use { cursor ->
                                    cursor.map { cursorToRecipient.map(it) }
                                }
                            }
                            .flatten()
                            .map { recipient ->
                                recipient.apply {
                                    contact = contactMatcher.findUniqueContact(address)
                                        ?.let { realm.copyFromRealm(it) }
                                }
                            }

                        conversation.apply {
                            recipients.clear()
                            recipients.addAll(matchedRecipients)

                            this.sendAsGroup =
                                if (recipients.size <= 1) false
                                else sendAsGroup

                            lastMessage = realm.where(Message::class.java)
                                .equalTo("threadId", threadId)
                                .isNull("deletedAt")
                                .sort("date", Sort.DESCENDING)
                                .findFirst()
                        }

                        realm.executeTransaction { it.insertOrUpdate(conversation) }
                    }
                }
        }

    /**
     * In some cases [createConversationFromCp] will return null if there are no messages present in the convo.
     * In order to allow the conversation to be accessed
     * we need to create an empty conversation in Realm to match the conversation created in the content provider.
     *
     * This is a bit of a hack, but is necessary on devices running HyperOS or variants.
     */
    private fun createEmptyConversation(threadId: Long, addresses: Collection<String>, sendAsGroup: Boolean): Conversation {
        Realm.getDefaultInstance().use { realm ->
            val realmContacts = realm.where(Contact::class.java).findAll()
            val contactMatcher = phoneNumberUtils.contactMatcher(realmContacts)
            val matchedRecipients = addresses.map { address ->
                Recipient().apply {
                    this.address = address
                    contact = contactMatcher.findUniqueContact(address)
                        ?.let { realm.copyFromRealm(it) }
                }
            }
            val conversation = Conversation().apply {
                id = threadId
                recipients.clear()
                recipients.addAll(matchedRecipients)
                this.sendAsGroup =
                    if (recipients.size <= 1) false
                    else sendAsGroup
            }
            realm.executeTransaction { it.copyToRealmOrUpdate(conversation) }
            return conversation
        }
    }

}
