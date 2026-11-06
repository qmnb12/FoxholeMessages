/*
 * Copyright (C) 2019 Moez Bhatti <moez.bhatti@gmail.com>
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
package dev.octoshrimpy.quik.util

import android.content.Context
import android.telephony.PhoneNumberUtils
import io.michaelrocks.libphonenumber.android.PhoneNumberUtil
import io.michaelrocks.libphonenumber.android.Phonenumber
import dev.octoshrimpy.quik.model.Contact
import java.util.Locale
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class PhoneNumberUtils @Inject constructor(context: Context) {

    private val countryCode = Locale.getDefault().country
    private val phoneNumberUtil = PhoneNumberUtil.createInstance(context)

    /**
     * Android's implementation is too loose and causes false positives
     * libphonenumber is stricter but too slow
     *
     * This method will run successfully stricter checks without compromising much speed
     */
    fun compare(first: String, second: String): Boolean {
        val normalizedFirst = normalizeNumber(first)
        val normalizedSecond = normalizeNumber(second)
        if (normalizedFirst.equals(normalizedSecond, true)) {
            return true
        }

        if (PhoneNumberUtils.compare(first, second)) {
            val matchType = phoneNumberUtil.isNumberMatch(normalizedFirst, normalizedSecond)
            if (matchType >= PhoneNumberUtil.MatchType.SHORT_NSN_MATCH) {
                return true
            }
        }

        return false
    }

    fun isPossibleNumber(number: CharSequence): Boolean {
        return parse(number) != null
    }

    fun isReallyDialable(digit: Char): Boolean {
        return PhoneNumberUtils.isReallyDialable(digit)
    }

    fun formatNumber(number: CharSequence): String {
        // PhoneNumberUtil doesn't maintain country code input
        return PhoneNumberUtils.formatNumber(number.toString(), countryCode) ?: number.toString()
    }

    fun normalizeNumber(number: String): String =
        number.filter { it.isLetterOrDigit() || it in "+*#" }

    /**
     * Builds a reusable contact index for batch recipient resolution.
     *
     * Call this once per sync batch, not once per recipient. In addition to avoiding repeated
     * Realm collection snapshots, the suffix index keeps normal phone-number lookups away from
     * a full scan of every contact and number. Candidate matches are still verified by compare()
     * so the index never decides a match by itself.
     */
    fun contactMatcher(contacts: Iterable<Contact>): ContactMatcher = ContactMatcher(contacts)

    /**
     * Convenience wrapper for one-off lookups. Batch callers should reuse [ContactMatcher].
     */
    fun findUniqueContact(address: String, contacts: Iterable<Contact>): Contact? =
        contactMatcher(contacts).findUniqueContact(address)

    private data class ContactNumberEntry(
        val contact: Contact,
        val address: String
    )

    inner class ContactMatcher internal constructor(contacts: Iterable<Contact>) {

        private val allEntries = ArrayList<ContactNumberEntry>()
        private val shortEntries = ArrayList<ContactNumberEntry>()
        private val entriesBySuffix = LinkedHashMap<String, MutableList<ContactNumberEntry>>()
        private val exactByAddress: Map<String, List<Contact>>

        init {
            val exactMatches = LinkedHashMap<String, LinkedHashMap<String, Contact>>()

            contacts.forEach { contact ->
                contact.numbers.forEach { number ->
                    val normalized = normalizeNumber(number.address).lowercase(Locale.ROOT)
                    if (normalized.isNotBlank()) {
                        exactMatches
                            .getOrPut(normalized) { LinkedHashMap() }[contact.lookupKey] = contact

                        val digits = normalized.filter { it.isDigit() }
                        val entry = ContactNumberEntry(contact, number.address)
                        allEntries += entry

                        if (digits.length < MIN_SUFFIX_DIGITS) {
                            shortEntries += entry
                        } else {
                            entriesBySuffix
                                .getOrPut(digits.takeLast(MIN_SUFFIX_DIGITS)) { ArrayList() }
                                .add(entry)
                        }
                    }
                }
            }

            exactByAddress = exactMatches.mapValues { (_, matches) -> matches.values.toList() }
        }

        fun findUniqueContact(address: String): Contact? {
            val normalizedAddress = normalizeNumber(address).lowercase(Locale.ROOT)
            if (normalizedAddress.isBlank()) return null

            exactByAddress[normalizedAddress]?.let { matches ->
                return matches.singleOrNull()
            }

            val digits = normalizedAddress.filter { it.isDigit() }
            val candidates = if (digits.length >= MIN_SUFFIX_DIGITS) {
                entriesBySuffix[digits.takeLast(MIN_SUFFIX_DIGITS)].orEmpty()
            } else {
                allEntries
            }

            val matches = LinkedHashMap<String, Contact>()

            fun collectMatches(entries: Iterable<ContactNumberEntry>): Boolean {
                for (entry in entries) {
                    if (compare(address, entry.address)) {
                        if (!matches.containsKey(entry.contact.lookupKey)) {
                            matches[entry.contact.lookupKey] = entry.contact
                            if (matches.size > 1) return false
                        }
                    }
                }
                return true
            }

            if (!collectMatches(candidates)) return null

            // Keep short numbers in the candidate set as well. Android and libphonenumber can
            // legitimately match some short/local numbers, so they must not be discarded by the
            // seven-digit suffix index.
            if (digits.length >= MIN_SUFFIX_DIGITS && !collectMatches(shortEntries)) return null

            return matches.values.singleOrNull()
        }
    }

    private companion object {
        const val MIN_SUFFIX_DIGITS = 7
    }

    private fun parse(number: CharSequence): Phonenumber.PhoneNumber? {
        return tryOrNull(false) { phoneNumberUtil.parse(number, countryCode) }
    }

}
