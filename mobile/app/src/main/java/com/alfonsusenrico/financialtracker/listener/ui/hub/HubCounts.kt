package com.alfonsusenrico.financialtracker.listener.ui.hub

import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionLists
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

/**
 * The status card's counts, from this phone's rows only and never money: notifications captured today, the
 * latest of them, and what still waits to reach the tracker. A count is null until Room has answered once.
 */
data class HubCounts(
    val capturedToday: Int?,
    val lastCaptureApp: String?,
    val lastCaptureAt: Long,
    val waiting: Int?,
    val oldestWaitingAt: Long?
) {
    companion object {
        private val WAITING_REPLIES = setOf("queued", "waiting_auth")
        private val WAITING_CAPTURES = setOf(TextCaptureState.QUEUED, TextCaptureState.SENDING)

        fun of(lists: CompanionLists, captures: List<TextCaptureEntity>?, today: LocalDate, zone: ZoneId = ZoneId.systemDefault()): HubCounts {
            if (!lists.loaded) return HubCounts(null, null, 0, null, null)
            val stored = (lists.visible + lists.local + lists.ignored).distinctBy { it.id }
            val todays = stored.filter { Instant.ofEpochMilli(it.postTime).atZone(zone).toLocalDate() == today }
            val last = todays.maxByOrNull { it.postTime }
            val unsent = lists.visible.filter { !it.isSynced && it.captureOutcome == "send" }
            val waitingCaptures = captures?.filter { it.serverId == null && it.state in WAITING_CAPTURES }
            val waitingReplies = lists.replies.filter { it.state in WAITING_REPLIES }
            val waiting = waitingCaptures?.let { unsent.size + it.size + waitingReplies.size }
            val oldest = (unsent.map { it.capturedAt } + waitingCaptures.orEmpty().map { it.createdAt }).minOrNull()
            return HubCounts(todays.size, last?.appLabel, last?.postTime ?: 0, waiting, oldest)
        }
    }
}
