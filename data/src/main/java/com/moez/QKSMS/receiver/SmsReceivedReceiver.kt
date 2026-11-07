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
package dev.octoshrimpy.quik.receiver

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.provider.Telephony.Sms
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.OutOfQuotaPolicy
import androidx.work.WorkManager
import androidx.work.workDataOf
import dagger.android.AndroidInjection
import dev.octoshrimpy.quik.repository.MessageRepository
import dev.octoshrimpy.quik.worker.ReceiveSmsWorker
import dev.octoshrimpy.quik.worker.ReceiveSmsWorker.Companion.INPUT_DATA_KEY_MESSAGE_ID
import io.reactivex.Single
import io.reactivex.schedulers.Schedulers
import timber.log.Timber
import javax.inject.Inject

class SmsReceivedReceiver : BroadcastReceiver() {
    @Inject lateinit var messageRepo: MessageRepository

    override fun onReceive(context: Context, intent: Intent) {
        AndroidInjection.inject(this, context)

        val messages = Sms.Intents.getMessagesFromIntent(intent)
            ?.takeIf { it.isNotEmpty() }
            ?: run {
                Timber.w("Ignoring SMS broadcast with no message parts")
                return
            }

        val pendingResult = goAsync()
        Single.just(messages)
            .observeOn(Schedulers.io())
            .map { parts ->
                Timber.v("onReceive() new sms")
                messageRepo.insertReceivedSms(
                    intent.extras?.getInt("subscription", -1) ?: -1,
                    parts[0].displayOriginatingAddress,
                    parts.joinToString(separator = "") { it.displayMessageBody.orEmpty() },
                    parts[0].timestampMillis
                ).id
            }
            .subscribe({ messageId ->
                try {
                    WorkManager.getInstance(context).enqueue(
                        OneTimeWorkRequestBuilder<ReceiveSmsWorker>()
                            .setInputData(workDataOf(INPUT_DATA_KEY_MESSAGE_ID to messageId))
                            .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
                            .build()
                    )
                } catch (error: Exception) {
                    Timber.e(error, "unable to enqueue SMS receive worker for messageId=$messageId")
                } finally {
                    pendingResult.finish()
                }
            }, { error ->
                Timber.e(error, "error receiving new sms")
                pendingResult.finish()
            })
    }

}