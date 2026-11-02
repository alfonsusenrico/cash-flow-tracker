package com.alfonsusenrico.financialtracker.listener.ui.model

import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

/**
 * Beranda's activity counts from this phone. They are never money: sums of this phone's rows miss what
 * was recorded on the web or another device and disagree with the tracker, so Beranda's only amounts are
 * the tracker's own cash-flow summary. A null count has not been read from Room yet and is never shown as 0.
 */
data class HomeFigures(
    /** Notifications stored today that the companion still lists: sent, kept on the phone or ignored. */
    val capturedToday: Int?,
    /** Today's notifications sent to the tracker, or about to be, and not yet settled as a record or as noise. */
    val waitingToday: Int?,
    /** Typed captures of today not yet recorded: queued, being sent, refused or asking. */
    val waitingCapturesToday: Int?
) {
    companion object {
        private val SETTLED = setOf("recorded", "ignored", "dismissed")

        /** [notifications] and [captures] are null until Room has answered once. */
        fun of(notifications: List<RawNotificationEntity>?, today: LocalDate, captures: List<CapturePresentation>?): HomeFigures {
            val todays = notifications?.distinctBy { it.id }?.filter { localDate(it.postTime) == today }
            return HomeFigures(
                capturedToday = todays?.size,
                waitingToday = todays?.count { it.captureOutcome == "send" && it.processingStatus !in SETTLED },
                waitingCapturesToday = captures?.count { it.isUnsettled && it.localDate == today }
            )
        }

        /** The "Perlu cek" tile: rows and captures not yet settled; null until both have been read. */
        fun needsCheck(visible: List<RawNotificationEntity>?, captures: List<CapturePresentation>?): Int? {
            if (visible == null || captures == null) return null
            return visible.count(ActivityFilter::needsCheck) + ActivityFilter.PENDING.selectCaptures(captures).size
        }

        /** The "Di ponsel" tile: rows and captures the tracker has not accepted; null until both have been read. */
        fun onPhone(local: List<RawNotificationEntity>?, captures: List<CapturePresentation>?): Int? {
            if (local == null || captures == null) return null
            return local.size + ActivityFilter.LOCAL.selectCaptures(captures).size
        }

        private fun localDate(millis: Long): LocalDate = Instant.ofEpochMilli(millis).atZone(ZoneId.systemDefault()).toLocalDate()
    }
}
