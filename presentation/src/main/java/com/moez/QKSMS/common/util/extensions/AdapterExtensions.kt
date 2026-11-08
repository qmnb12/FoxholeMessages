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
package dev.octoshrimpy.quik.common.util.extensions

import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView

fun RecyclerView.Adapter<*>.autoScrollToStart(recyclerView: RecyclerView) {
    fun scrollToLatestIfAlreadyAtEnd() {
        val layoutManager = recyclerView.layoutManager as? LinearLayoutManager ?: return
        if (!layoutManager.stackFromEnd || getItemCount() == 0) return

        // The adapter count has already changed when RecyclerView dispatches this callback.
        // Before layout runs, the old last visible row is therefore at most itemCount - 2.
        // Only follow the new last message when the user was already at the end.
        val lastVisible = layoutManager.findLastVisibleItemPosition()
        val wasAtEnd = lastVisible == RecyclerView.NO_POSITION ||
            lastVisible >= getItemCount() - 2

        if (wasAtEnd) {
            recyclerView.post {
                val latestPosition = getItemCount() - 1
                if (latestPosition >= 0) recyclerView.scrollToPosition(latestPosition)
            }
        }
    }

    registerAdapterDataObserver(object : RecyclerView.AdapterDataObserver() {
        override fun onItemRangeInserted(positionStart: Int, itemCount: Int) {
            val layoutManager = recyclerView.layoutManager as? LinearLayoutManager ?: return

            if (layoutManager.stackFromEnd) {
                if (positionStart > 0) notifyItemChanged(positionStart - 1)
                scrollToLatestIfAlreadyAtEnd()
            } else {
                val firstVisiblePosition = layoutManager.findFirstVisibleItemPosition()
                if (firstVisiblePosition == 0) {
                    recyclerView.post { recyclerView.scrollToPosition(positionStart) }
                }
            }
        }

        override fun onItemRangeChanged(positionStart: Int, itemCount: Int) {
            val layoutManager = recyclerView.layoutManager as? LinearLayoutManager ?: return
            if (layoutManager.stackFromEnd) {
                scrollToLatestIfAlreadyAtEnd()
            } else {
                onItemRangeInserted(positionStart, itemCount)
            }
        }

        override fun onChanged() {
            // Realm's adapter can dispatch a full refresh instead of a granular insert event.
            // Handle that path too, otherwise newly-sent messages remain below the viewport
            // until the conversation is reopened.
            scrollToLatestIfAlreadyAtEnd()
        }
    })
}