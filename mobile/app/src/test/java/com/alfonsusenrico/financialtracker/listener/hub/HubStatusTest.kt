package com.alfonsusenrico.financialtracker.listener.hub

import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import com.alfonsusenrico.financialtracker.listener.ui.hub.FixAction
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubInputs
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubStatus
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHints
import com.alfonsusenrico.financialtracker.listener.ui.hub.ProblemKind
import com.alfonsusenrico.financialtracker.listener.ui.hub.ProblemTone
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

/** The status card's every state: calm, each problem with its fix, the softer notes, and never money. */
class HubStatusTest {
    private val zone = ZoneId.of("Asia/Jakarta")
    private val day = LocalDate.of(2026, 11, 2)
    private fun at(hour: Int, minute: Int = 0) = day.atTime(LocalTime.of(hour, minute)).atZone(zone).toInstant().toEpochMilli()

    private val calm = HubInputs(
        paired = true, notificationAccess = true, listenerConnected = true, listenerDisconnectedAt = 0, listenerConnectedAt = at(7, 2),
        serverContact = ServerContact.OK, online = true, autoSync = true, batteryUnrestricted = true, postNotifications = true,
        capturedToday = 7, lastCaptureApp = "GoPay", lastCaptureAt = at(12, 41), lastSyncAt = at(12, 42), waiting = 0,
        oldestWaitingAt = null, selfTestPassedAt = 0, oem = null, now = at(14, 5), zone = zone
    )

    private fun status(input: HubInputs) = HubStatus.of(input)

    @Test fun calmStatusCountsCapturesAndNeverMoney() {
        val status = status(calm)
        assertNull(status.problem)
        assertEquals("Lagi jagain notifikasimu", status.title)
        assertEquals("7 notifikasi ketangkep hari ini · terakhir GoPay, 12.41", status.todayLine)
        assertEquals("Kekirim semua, 12.42", status.syncLine)
        assertTrue(status.syncOk)
        assertTrue(status.softIssues.isEmpty())
        assertFalse(listOf(status.title, status.todayLine, status.syncLine).any { "Rp" in it })
    }

    @Test fun countsNotYetReadAreNeverZero() {
        val status = status(calm.copy(capturedToday = null, waiting = null))
        assertEquals("Tangkapan hari ini belum kebaca", status.todayLine)
        assertEquals("Antrean belum kebaca", status.syncLine)
        assertNull("An unread count is not a quiet day", status.problem)
    }

    @Test fun notPairedAsksToPair() {
        val problem = status(calm.copy(paired = false)).problem!!
        assertEquals(ProblemKind.NOT_PAIRED, problem.kind)
        assertEquals(FixAction.PAIR, problem.fix)
        assertEquals("Sambungin", problem.fixLabel)
    }

    @Test fun refusedKeyAsksToPairAgainBeforeAnythingElse() {
        val problem = status(calm.copy(serverContact = ServerContact.REFUSED, notificationAccess = false)).problem!!
        assertEquals(ProblemKind.KEY_REFUSED, problem.kind)
        assertEquals(ProblemTone.BAD, problem.tone)
        assertEquals("Sambungan ke akunmu putus", problem.title)
        assertEquals("Sambungin ulang", problem.fixLabel)
        assertEquals(FixAction.PAIR, problem.fix)
        assertEquals("Kunci ditolak server", status(calm.copy(serverContact = ServerContact.REFUSED)).syncLine)
    }

    @Test fun lostNotificationAccessOpensAndroidsAccessPage() {
        val problem = status(calm.copy(notificationAccess = false, listenerConnected = false)).problem!!
        assertEquals(ProblemKind.ACCESS_OFF, problem.kind)
        assertEquals("Aku nggak bisa denger notifikasi", problem.title)
        assertEquals(FixAction.OPEN_ACCESS, problem.fix)
        assertEquals("Nyalain lagi", problem.fixLabel)
    }

    @Test fun aFrozenListenerOnHiosNamesTheBatterySaverAndItsSteps() {
        val hios = OemHints.of("TECNO")
        val problem = status(calm.copy(listenerConnected = false, listenerDisconnectedAt = at(9, 40), oem = hios)).problem!!
        assertEquals(ProblemKind.LISTENER_ASLEEP, problem.kind)
        assertEquals("HP-mu nidurin aku", problem.title)
        assertEquals("Penghemat baterai HiOS ngebekuin aplikasi ini dari jam 09.40. Notifikasi setelah itu bisa kelewat.", problem.body)
        assertEquals(FixAction.OPEN_BATTERY, problem.fix)
        assertEquals("Atur baterai", problem.fixLabel)
        assertTrue(problem.oemHint!!.steps.contains("Phone Master"))
        // The battery note is the fix itself here, so it is not repeated as a soft issue.
        assertTrue(status(calm.copy(listenerConnected = false, batteryUnrestricted = false)).softIssues.none { it.fix == FixAction.OPEN_BATTERY })
    }

    @Test fun aDisconnectedListenerWithBatteryAllowedReconnects() {
        val problem = status(calm.copy(listenerConnected = false, listenerDisconnectedAt = at(9, 40))).problem!!
        assertEquals(ProblemKind.LISTENER_ASLEEP, problem.kind)
        assertEquals("Aku lagi putus dari notifikasi", problem.title)
        assertEquals(FixAction.RECONNECT_LISTENER, problem.fix)
        assertNull(problem.oemHint)
    }

    @Test fun anUnreachableServerSaysTheWaitingItemsAreSafe() {
        val problem = status(calm.copy(serverContact = ServerContact.UNREACHABLE, waiting = 3)).problem!!
        assertEquals(ProblemKind.SERVER_UNREACHABLE, problem.kind)
        assertEquals(ProblemTone.WARN, problem.tone)
        assertEquals("Tenang, 3 yang nunggu aman di HP. Aku kirim begitu nyambung.", problem.body)
        assertEquals(FixAction.RETRY_SERVER, problem.fix)
        assertEquals("Coba lagi", problem.fixLabel)
    }

    @Test fun offlineWithItemsWaitingOffersToSendNow() {
        val problem = status(calm.copy(online = false, waiting = 2, oldestWaitingAt = at(13, 0))).problem!!
        assertEquals(ProblemKind.WAITING_TO_SEND, problem.kind)
        assertEquals("Lagi offline", problem.title)
        assertEquals(FixAction.SEND_NOW, problem.fix)
        assertEquals("Offline, 2 nunggu dikirim", status(calm.copy(online = false, waiting = 2)).syncLine)
    }

    @Test fun itemsStuckOnlineForMoreThanAQuarterHourOfferToSendNow() {
        assertNull(status(calm.copy(waiting = 2, oldestWaitingAt = at(14, 0))).problem)
        val problem = status(calm.copy(waiting = 2, oldestWaitingAt = at(13, 40))).problem!!
        assertEquals(ProblemKind.WAITING_TO_SEND, problem.kind)
        assertEquals("Ada 2 yang belum kekirim", problem.title)
        assertEquals("Kirim sekarang", problem.fixLabel)
        // With auto-sync off, waiting is the owner's choice: a soft note, not a problem.
        val manual = status(calm.copy(waiting = 2, oldestWaitingAt = at(13, 40), autoSync = false))
        assertNull(manual.problem)
        assertEquals(FixAction.SEND_NOW, manual.softIssues.single().fix)
        assertEquals("Kirim otomatis mati · 2 nunggu dikirim", manual.syncLine)
    }

    @Test fun aQuietAfternoonWithTheListenerOnOffersTheSelfTest() {
        val problem = status(calm.copy(capturedToday = 0)).problem!!
        assertEquals(ProblemKind.QUIET_DAY, problem.kind)
        assertEquals("Hari ini masih sepi", problem.title)
        assertEquals(FixAction.RUN_SELF_TEST, problem.fix)
        assertEquals("Tes sekarang", problem.fixLabel)
        assertEquals("Belum ada notifikasi yang ketangkep hari ini", status(calm.copy(capturedToday = 0)).todayLine)
        // Before midday a quiet morning is normal, and a test that passed today settles it.
        assertNull(status(calm.copy(capturedToday = 0, now = at(11, 0))).problem)
        assertNull(status(calm.copy(capturedToday = 0, selfTestPassedAt = at(13, 0))).problem)
        assertNotNull(status(calm.copy(capturedToday = 0, selfTestPassedAt = at(13, 0) - 24 * 3_600_000L)).problem)
    }

    @Test fun softIssuesSayWhatIsMissingWithTheirFix() {
        val status = status(calm.copy(batteryUnrestricted = false, postNotifications = false, autoSync = false))
        assertNull(status.problem)
        assertEquals(
            listOf(FixAction.OPEN_BATTERY, FixAction.OPEN_NOTIFICATION_SETTINGS, FixAction.SEND_NOW),
            status.softIssues.map { it.fix }
        )
        assertEquals("Notifikasi dariku mati. Pertanyaan cuma ada di aplikasi.", status.softIssues[1].text)
    }

    @Test fun oemHintsCoverTheCommonBatterySavers() {
        assertEquals("HiOS", OemHints.of("tecno")!!.system)
        assertEquals("HiOS", OemHints.of("Infinix")!!.system)
        assertEquals("ColorOS", OemHints.of("realme")!!.system)
        assertEquals("MIUI/HyperOS", OemHints.of("Xiaomi")!!.system)
        assertEquals("One UI", OemHints.of("samsung")!!.system)
        assertNull(OemHints.of("Google"))
        assertTrue(OemHints.freezesApps(OemHints.of("TECNO")))
        assertFalse(OemHints.freezesApps(OemHints.of("samsung")))
    }
}
