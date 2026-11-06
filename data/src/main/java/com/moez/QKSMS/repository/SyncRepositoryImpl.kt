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

import android.content.ContentResolver
import android.content.ContentUris
import android.net.Uri
import android.os.Handler
import android.os.HandlerThread
import android.provider.Telephony
import com.f2prateek.rx.preferences2.RxSharedPreferences
import dev.octoshrimpy.quik.extensions.forEach
import dev.octoshrimpy.quik.extensions.insertOrUpdate
import dev.octoshrimpy.quik.extensions.map
import dev.octoshrimpy.quik.manager.KeyManager
import dev.octoshrimpy.quik.mapper.CursorToContact
import dev.octoshrimpy.quik.mapper.CursorToContactGroup
import dev.octoshrimpy.quik.mapper.CursorToContactGroupMember
import dev.octoshrimpy.quik.mapper.CursorToConversation
import dev.octoshrimpy.quik.mapper.CursorToMessage
import dev.octoshrimpy.quik.mapper.CursorToPart
import dev.octoshrimpy.quik.mapper.CursorToRecipient
import dev.octoshrimpy.quik.model.Contact
import dev.octoshrimpy.quik.model.ContactGroup
import dev.octoshrimpy.quik.model.Conversation
import dev.octoshrimpy.quik.model.EmojiReaction
import dev.octoshrimpy.quik.model.Message
import dev.octoshrimpy.quik.model.MmsPart
import dev.octoshrimpy.quik.model.PhoneNumber
import dev.octoshrimpy.quik.model.Recipient
import dev.octoshrimpy.quik.model.SyncLog
import dev.octoshrimpy.quik.interactor.DeduplicateMessages
import dev.octoshrimpy.quik.util.PhoneNumberUtils
import dev.octoshrimpy.quik.util.tryOrNull
import io.reactivex.Completable
import io.reactivex.CompletableEmitter
import io.reactivex.android.schedulers.AndroidSchedulers
import io.reactivex.subjects.BehaviorSubject
import io.reactivex.subjects.Subject
import io.realm.Realm
import io.realm.RealmList
import io.realm.Sort
import timber.log.Timber
import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton

@Singleton
class SyncRepositoryImpl @Inject constructor(
    private val contentResolver: ContentResolver,
    private val conversationRepo: ConversationRepository,
    private val cursorToConversation: CursorToConversation,
    private val cursorToMessage: CursorToMessage,
    private val cursorToPart: CursorToPart,
    private val cursorToRecipient: CursorToRecipient,
    private val cursorToContact: CursorToContact,
    private val cursorToContactGroup: CursorToContactGroup,
    private val cursorToContactGroupMember: CursorToContactGroupMember,
    private val keys: KeyManager,
    private val phoneNumberUtils: PhoneNumberUtils,
    private val messageRepo: Provider<MessageRepository>,
    private val rxPrefs: RxSharedPreferences,
    private val reactions: EmojiReactionRepository,
) : SyncRepository {

    override val syncProgress: Subject<SyncRepository.SyncProgress> =
            BehaviorSubject.createDefault(SyncRepository.SyncProgress.Idle)

    // Keep the full rebuild guarded through emoji parsing. ParsingEmojis is still part of the
    // same operation and must not admit a second destructive rebuild.
    private val syncLock = Any()
    private var syncInProgress = false
    private var incrementalSyncObservedDuringFullSync = false

    private val activeSyncWaiters = mutableListOf<CompletableEmitter>()
    private val queuedSyncWaiters = mutableListOf<CompletableEmitter>()

    /**
     * Finish the current rebuild. Restore requests made during an existing sync are queued
     * separately and trigger another rebuild before their Completable is completed.
     */
    private fun finishSync(error: Throwable? = null) {
        val (completedWaiters, startQueuedSync) = synchronized(syncLock) {
            val completed = activeSyncWaiters.toList()
            activeSyncWaiters.clear()
            queuedSyncWaiters.removeAll { emitter -> emitter.isDisposed }

            val startNext = queuedSyncWaiters.isNotEmpty() || incrementalSyncObservedDuringFullSync
            incrementalSyncObservedDuringFullSync = false
            if (startNext) {
                activeSyncWaiters.addAll(queuedSyncWaiters)
                queuedSyncWaiters.clear()
                syncProgress.onNext(SyncRepository.SyncProgress.Running(0, 0, true))
            } else {
                syncInProgress = false
                syncProgress.onNext(SyncRepository.SyncProgress.Idle)
            }

            Pair(completed, startNext)
        }

        completedWaiters.forEach { emitter ->
            if (!emitter.isDisposed) {
                if (error == null) emitter.onComplete() else emitter.onError(error)
            }
        }

        if (startQueuedSync) performSync()
    }

    override fun syncMessages() {
        val started = synchronized(syncLock) {
            if (syncInProgress) {
                false
            } else {
                syncInProgress = true
                syncProgress.onNext(SyncRepository.SyncProgress.Running(0, 0, true))
                true
            }
        }

        if (started) performSync()
    }

    override fun syncMessagesAndAwait(): Completable = Completable.create { emitter ->
        val shouldStart = synchronized(syncLock) {
            if (syncInProgress) {
                queuedSyncWaiters.add(emitter)
                false
            } else {
                syncInProgress = true
                activeSyncWaiters.add(emitter)
                syncProgress.onNext(SyncRepository.SyncProgress.Running(0, 0, true))
                true
            }
        }

        emitter.setCancellable {
            synchronized(syncLock) {
                activeSyncWaiters.remove(emitter)
                queuedSyncWaiters.remove(emitter)
            }
        }

        if (shouldStart) performSync()
    }

    private fun performSync() {
        val oldBlockedSenders = try {
            rxPrefs.getStringSet("pref_key_blocked_senders")
        } catch (error: Exception) {
            Timber.e(error, "Unable to read blocked-sender migration preference")
            finishSync(error)
            return
        }

        val handlerThread = HandlerThread("RealmSyncThread")
        try {
            handlerThread.start()
        } catch (error: Exception) {
            Timber.e(error, "Unable to start Realm sync thread")
            finishSync(error)
            return
        }

        val taskPosted = Handler(handlerThread.looper).post {
            // Read the contacts provider before opening the long Realm write transaction.
            val syncedContacts = try {
                getContacts()
            } catch (error: Exception) {
                handlerThread.quitSafely()
                Timber.e(error, "Unable to read contacts for full sync")
                finishSync(error)
                return@post
            }

            val syncRealm = try {
                Realm.getDefaultInstance()
            } catch (error: Exception) {
                handlerThread.quitSafely()
                Timber.e(error, "Unable to open Realm for full sync")
                finishSync(error)
                return@post
            }
            try {
                syncRealm.executeTransactionAsync(
                { realm ->
                // Prepare existing conversation data
                val persistedData = realm.copyFromRealm(
                    realm.where(Conversation::class.java)
                        .beginGroup()
                        .equalTo("archived", true)
                        .or()
                        .equalTo("blocked", true)
                        .or()
                        .equalTo("pinned", true)
                        .or()
                        .isNotEmpty("name")
                        .or()
                        .isNotNull("blockingClient")
                        .or()
                        .isNotEmpty("blockReason")
                        .or()
                        .isNotNull("categoryOverride")
                        .or()
                        .equalTo("sendAsGroup", false)
                        .endGroup()
                        .findAll()
                ).associateBy { conversation -> conversation.id }.toMutableMap()

                // Full sync rebuilds Realm messages from Android's provider. Snapshot state
                // owned by Foxhole first, using the provider identity which survives a rebuild.
                val persistedMessageState = realm.where(Message::class.java)
                    .findAll()
                    .filter { message -> message.contentId != 0L }
                    .associate { message ->
                        (message.type to message.contentId) to MessageSyncState.from(message)
                    }

                val providerCursors = mutableListOf<android.database.Cursor>()
                try {
                    // Acquire every provider cursor before clearing local state. If any
                    // provider query returns null or throws, already-open cursors are closed
                    // by this finally block and the Realm rebuild is rolled back.
                    val partsCursor = cursorToPart.getPartsCursor()
                        ?.also { cursor -> providerCursors.add(cursor) }
                        ?: throw IllegalStateException("MMS parts provider cursor unavailable")
                    val messageCursor = cursorToMessage.getMessagesCursor()
                        ?.also { cursor -> providerCursors.add(cursor) }
                        ?: throw IllegalStateException("Messages provider cursor unavailable")
                    val conversationCursor = cursorToConversation.getConversationsCursor()
                        ?.also { cursor -> providerCursors.add(cursor) }
                        ?: throw IllegalStateException("Conversations provider cursor unavailable")
                    val recipientCursor = cursorToRecipient.getRecipientCursor()
                        ?.also { cursor -> providerCursors.add(cursor) }
                        ?: throw IllegalStateException("Recipients provider cursor unavailable")

                removeOldMessages(realm)
                keys.reset()

                val max = (partsCursor?.count ?: 0) +
                        (messageCursor?.count ?: 0) +
                        (conversationCursor?.count ?: 0) +
                        (recipientCursor?.count ?: 0)

                var progress = 0

                // Sync message parts
                partsCursor?.use {
                    partsCursor.forEach { cursor ->
                        tryOrNull {
                            val part = cursorToPart.map(partsCursor)
                            realm.insertOrUpdate(part)
                            progress++
                        }
                    }
                }

                // Build the MMS-part lookup once. Querying Realm once for every MMS message
                // made large full syncs perform an avoidable N+1 sequence of database queries.
                val partsByMessageId = realm.where(MmsPart::class.java)
                    .findAll()
                    .groupBy { part -> part.messageId }

                // Sync messages
                messageCursor?.use {
                    val messageColumns = CursorToMessage.MessageColumns(messageCursor)
                    messageCursor.forEach { cursor ->
                        tryOrNull {
                            syncProgress.onNext(
                                SyncRepository.SyncProgress.Running(
                                    max,
                                    ++progress,
                                    false
                                )
                            )
                            val message = cursorToMessage.map(Pair(cursor, messageColumns)).apply {
                                persistedMessageState[type to contentId]?.applyTo(this)
                                if (isMms()) {
                                    parts = RealmList<MmsPart>().apply {
                                        addAll(
                                            partsByMessageId[contentId].orEmpty()
                                        )
                                    }
                                }
                            }
                            realm.insertOrUpdate(message)
                        }
                    }
                }

                // Precompute each thread's newest non-trashed message once. Previously the
                // conversation cursor loop issued one Realm query per conversation.
                val latestMessagesByThreadId = HashMap<Long, Message>()
                realm.where(Message::class.java)
                    .isNull("deletedAt")
                    .sort("date", Sort.DESCENDING)
                    .findAll()
                    .forEach { message ->
                        if (!latestMessagesByThreadId.containsKey(message.threadId)) {
                            latestMessagesByThreadId[message.threadId] = message
                        }
                    }

                // Migrate blocked conversations from 2.7.3
                oldBlockedSenders.get()
                    .mapNotNull { threadIdString -> threadIdString.toLongOrNull() }
                    .filter { threadId -> !persistedData.contains(threadId) }
                    .forEach { threadId ->
                        persistedData[threadId] = Conversation(id = threadId, blocked = true)
                    }

                // Sync conversations
                conversationCursor?.use {
                    conversationCursor.forEach { cursor ->
                        tryOrNull {
                            syncProgress.onNext(
                                SyncRepository.SyncProgress.Running(
                                    max,
                                    ++progress,
                                    false
                                )
                            )
                            val conversation = cursorToConversation.map(cursor).apply {
                                persistedData[id]?.let { persistedConversation ->
                                    archived = persistedConversation.archived
                                    blocked = persistedConversation.blocked
                                    pinned = persistedConversation.pinned
                                    name = persistedConversation.name
                                    blockingClient = persistedConversation.blockingClient
                                    blockReason = persistedConversation.blockReason
                                    sendAsGroup = persistedConversation.sendAsGroup
                                    categoryOverride = persistedConversation.categoryOverride
                                }
                                lastMessage = latestMessagesByThreadId[id]
                            }
                            realm.insertOrUpdate(conversation)
                        }
                    }
                }

                // Sync recipients
                val contacts = realm.copyToRealmOrUpdate(syncedContacts)
                val contactMatcher = phoneNumberUtils.contactMatcher(contacts)
                recipientCursor?.use {
                    recipientCursor.forEach { cursor ->
                        tryOrNull {
                            syncProgress.onNext(
                                SyncRepository.SyncProgress.Running(
                                    max,
                                    ++progress,
                                    false
                                )
                            )
                            val rec = cursorToRecipient.map(cursor).apply {
                                contact = contactMatcher.findUniqueContact(address)
                            }
                            realm.insertOrUpdate(rec)
                        }
                    }
                }

                syncProgress.onNext(SyncRepository.SyncProgress.ParsingEmojis(0, 0, true))

                // Now that we have all the messages, we can scan for emoji reactions
                reactions.deleteAndReparseAllEmojiReactions(
                    realm,
                    onProgress = { progress ->
                        syncProgress.onNext(progress)
                    })

                if (rxPrefs.getBoolean("autoDeduplicateMessages").get()) {
                    DeduplicateMessages(messageRepo.get())
                        .buildObservable(Unit)
                        .observeOn(AndroidSchedulers.mainThread())
                        .subscribe { result ->
                            when (result) {
                                is MessageRepository.DeduplicationResult.NoDuplicates -> {
                                    Timber.i("No duplicate messages found.")
                                }
                                is MessageRepository.DeduplicationResult.Success -> {
                                    Timber.i("Deleted duplicate messages")
                                }
                                is MessageRepository.DeduplicationResult.Failure -> {
                                    Timber.e(result.error, "Deduplication failed")
                                }
                            }
                        }
                }

                realm.insert(SyncLog())
                } finally {
                    providerCursors.forEach { cursor ->
                        try {
                            if (!cursor.isClosed) cursor.close()
                        } catch (closeError: Exception) {
                            Timber.w(closeError, "Unable to close provider cursor after sync")
                        }
                    }
                }
                }, {
                    try {
                        oldBlockedSenders.delete()
                    } catch (error: Exception) {
                        Timber.w(error, "Unable to clear migrated blocked-sender preference")
                    } finally {
                        try {
                            syncRealm.close()
                        } catch (closeError: Exception) {
                            Timber.w(closeError, "Unable to close Realm after sync")
                        }
                        handlerThread.quitSafely()
                        finishSync()
                    }
                },
                { error ->
                    try {
                        syncRealm.close()
                    } catch (closeError: Exception) {
                        Timber.w(closeError, "Unable to close Realm after failed sync")
                    }
                    handlerThread.quitSafely()
                    Timber.e(error, "syncMessages Failed")
                    finishSync(error)
                })
            } catch (error: Exception) {
                try {
                    syncRealm.close()
                } catch (closeError: Exception) {
                    Timber.w(closeError, "Unable to close Realm after sync scheduling failure")
                }
                handlerThread.quitSafely()
                Timber.e(error, "Unable to schedule syncMessages transaction")
                finishSync(error)
            }
        }
        if (!taskPosted) {
            handlerThread.quitSafely()
            finishSync(IllegalStateException("Unable to schedule full sync on Realm thread"))
        }
    }

    override fun syncMessage(uri: Uri, messageId: Long): Message? {

        // If we don't have a valid type, return null
        val type = when {
            uri.toString().contains(Message.TYPE_MMS) -> Message.TYPE_MMS
            uri.toString().contains(Message.TYPE_SMS) -> Message.TYPE_SMS
            else -> return null
        }

        // If we don't have a valid id, return null
        val contentId = tryOrNull(false) { ContentUris.parseId(uri) } ?: return null

        // A full rebuild may already have captured its provider cursors. Remember incremental
        // writes that arrive during that window and run one fresh full pass after this rebuild.
        synchronized(syncLock) {
            if (syncInProgress) incrementalSyncObservedDuringFullSync = true
        }

        // Check if the message already exists, so we can reuse the id
        val existingMessageState = Realm.getDefaultInstance().use { realm ->
            realm.refresh()
            realm.where(Message::class.java)
                .equalTo("type", type)
                .equalTo("contentId", contentId)
                .or()
                .beginGroup()
                .equalTo("id", messageId)
                .and()
                .equalTo("contentId", 0L)
                .endGroup()
                .findFirst()
                ?.let { message -> message.id to MessageSyncState.from(message) }
        }

        // The uri might be something like content://mms/inbox/id
        // The box might change though, so we should just use the mms/id uri
        val stableUri = when (type) {
            Message.TYPE_MMS -> ContentUris.withAppendedId(Telephony.Mms.CONTENT_URI, contentId)
            else -> ContentUris.withAppendedId(Telephony.Sms.CONTENT_URI, contentId)
        }

        return contentResolver.query(stableUri, null, null, null, null)?.use { cursor ->

            // If there are no rows, return null. Otherwise, we've moved to the first row
            if (!cursor.moveToFirst()) return null

            val columnsMap = CursorToMessage.MessageColumns(cursor)
            cursorToMessage.map(Pair(cursor, columnsMap)).apply {
                existingMessageState?.let { (existingId, localState) ->
                    this.id = existingId
                    localState.applyTo(this)
                }

                if (isMms()) {
                    // A null cursor means the provider lookup failed, not that this MMS has no
                    // attachments. Leave the existing Realm message untouched on that failure.
                    val partsCursor = cursorToPart.getPartsCursor(contentId) ?: return null
                    parts = RealmList<MmsPart>().apply {
                        addAll(partsCursor.use { cursor -> cursor.map(cursorToPart::map) })
                    }
                }

                conversationRepo.getOrCreateConversation(threadId)
                insertOrUpdate()

                val text = getText(false)
                val parsedReaction = reactions.parseEmojiReaction(text)
                if (parsedReaction != null) {
                    Realm.getDefaultInstance().use { realm ->
                        val targetMessage = reactions.findTargetMessage(
                            this,
                            parsedReaction,
                            realm
                        )
                        realm.executeTransaction {
                            reactions.saveEmojiReaction(
                                this,
                                parsedReaction,
                                targetMessage,
                                realm,
                            )
                        }
                    }
                }
            }
        }
    }

    override fun syncContacts() {
        // Keep cross-process ContactsProvider reads outside the Realm write transaction. Holding
        // Realm's write lock while querying the provider makes every other Realm writer wait.
        val contacts = getContacts()
        val contactGroups = getContactGroups(contacts)

        Realm.getDefaultInstance().use { realm ->
            val recipients = realm.where(Recipient::class.java).findAll()

            realm.executeTransaction {
                realm.delete(Contact::class.java)
                realm.delete(ContactGroup::class.java)

                val managedContacts = realm.copyToRealmOrUpdate(contacts)
                val contactMatcher = phoneNumberUtils.contactMatcher(managedContacts)
                val contactsByLookupKey = managedContacts.associateBy { contact -> contact.lookupKey }

                // Reconnect group members to the Contact objects managed by this Realm.
                contactGroups.forEach { group ->
                    val lookupKeys = group.contacts.map { contact -> contact.lookupKey }
                    group.contacts.clear()
                    group.contacts.addAll(lookupKeys.mapNotNull { lookupKey -> contactsByLookupKey[lookupKey] })
                }
                realm.insertOrUpdate(contactGroups)

                // Update all the recipients with the new contacts
                recipients.forEach { recipient ->
                    recipient.contact = contactMatcher.findUniqueContact(recipient.address)
                }

                realm.insertOrUpdate(recipients)
            }
        }
    }

    private fun getContacts(): List<Contact> {
        val defaultNumberIds = Realm.getDefaultInstance().use { realm ->
            realm.where(PhoneNumber::class.java)
                    .equalTo("isDefault", true)
                    .findAll()
                    .map { number -> number.id }
                    .toHashSet()
        }

        // Cursor.map() materializes rows but does not close its provider cursor. A null cursor
        // means the provider query failed; treating it as an empty address book could erase data.
        val contactsCursor = cursorToContact.getContactsCursor()
            ?: throw IllegalStateException("Contacts provider cursor unavailable")

        return contactsCursor.use { cursor ->
            cursor.map(cursorToContact::map)
                .groupBy { contact -> contact.lookupKey }
                .map { contacts ->
                    // Some providers expose duplicate phone-number rows for linked accounts.
                    val uniqueNumbers = mutableListOf<PhoneNumber>()
                    contacts.value
                        .flatMap { it.numbers }
                        .forEach { number ->
                            number.isDefault = number.id in defaultNumberIds
                            val duplicate = uniqueNumbers.find { other ->
                                phoneNumberUtils.compare(number.address, other.address)
                            }

                            if (duplicate == null) {
                                uniqueNumbers += number
                            } else if (!duplicate.isDefault && number.isDefault) {
                                duplicate.isDefault = true
                            }
                        }

                    contacts.value.first().apply {
                        numbers.clear()
                        numbers.addAll(uniqueNumbers)
                    }
                }
        }
    }

    private fun getContactGroups(contacts: List<Contact>): List<ContactGroup> {
        val membersCursor = cursorToContactGroupMember.getGroupMembersCursor()
            ?: throw IllegalStateException("Contact group-member cursor unavailable")
        val groupMembers = membersCursor.use { cursor ->
            cursor.map(cursorToContactGroupMember::map)
        }

        val groupsCursor = cursorToContactGroup.getContactGroupsCursor()
            ?: throw IllegalStateException("Contact groups cursor unavailable")
        val groups = groupsCursor.use { cursor ->
            cursor.map(cursorToContactGroup::map)
        }

        // Index both sides once instead of scanning every member and contact for every group.
        val contactsByLookupKey = contacts.associateBy { contact -> contact.lookupKey }
        val membersByGroupId = groupMembers.groupBy { member -> member.groupId }

        groups.forEach { group ->
            group.contacts.addAll(
                membersByGroupId[group.id]
                    .orEmpty()
                    .mapNotNull { member -> contactsByLookupKey[member.lookupKey] }
            )
        }

        return groups
    }

    private fun removeOldMessages(realm: Realm) {
        realm.delete(Contact::class.java)
        realm.delete(ContactGroup::class.java)
        realm.delete(Conversation::class.java)
        realm.delete(Message::class.java)
        realm.delete(MmsPart::class.java)
        realm.delete(Recipient::class.java)
    }

}
