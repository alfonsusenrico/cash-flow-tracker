package com.alfonsusenrico.financialtracker.listener.ui.hub

import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/** What one tap on the status card's fix button does. */
enum class FixAction {
    PAIR, OPEN_ACCESS, OPEN_BATTERY, RECONNECT_LISTENER, RETRY_SERVER, SEND_NOW, RUN_SELF_TEST, OPEN_NOTIFICATION_SETTINGS
}

enum class ProblemKind { NOT_PAIRED, KEY_REFUSED, ACCESS_OFF, LISTENER_ASLEEP, SERVER_UNREACHABLE, WAITING_TO_SEND, QUIET_DAY }

/** A bad problem stops capture; a warning means captures are safe on the phone but something waits. */
enum class ProblemTone { BAD, WARN }

data class Problem(
    val kind: ProblemKind,
    val tone: ProblemTone,
    val title: String,
    val body: String,
    val fixLabel: String,
    val fix: FixAction,
    /** Steps for this phone's battery saver, shown under the body; null when none are known. */
    val oemHint: OemHint? = null
)

data class SoftIssue(val text: String, val fixLabel: String, val fix: FixAction)

/** A battery saver that freezes background apps, and how to exempt this app on that phone. */
data class OemHint(val brand: String, val system: String, val steps: String)

object OemHints {
    fun of(manufacturer: String?): OemHint? = when (manufacturer?.trim()?.lowercase()) {
        "tecno", "infinix", "itel" -> OemHint(
            manufacturer.trim().uppercase().takeIf { it == "TECNO" } ?: manufacturer.trim().replaceFirstChar { it.uppercase() }, "HiOS",
            "Buka Phone Master → Kelola peluncuran aplikasi → pilih Cash Flow Tracker, matiin Kelola otomatis, " +
                "terus nyalain Luncur otomatis, Luncur sekunder, dan Jalan di latar."
        )
        "xiaomi", "redmi", "poco" -> OemHint(
            manufacturer.trim().replaceFirstChar { it.uppercase() }, "MIUI/HyperOS",
            "Info aplikasi → Mulai otomatis nyala, terus Penghemat baterai → Tanpa batasan."
        )
        "oppo", "realme", "oneplus" -> OemHint(
            manufacturer.trim().replaceFirstChar { it.uppercase() }, "ColorOS",
            "Info aplikasi → Penggunaan baterai → Izinkan aktivitas latar."
        )
        "samsung" -> OemHint("Samsung", "One UI", "Perawatan perangkat → Baterai → jangan masukin aku ke Aplikasi tidur.")
        else -> null
    }

    fun freezesApps(hint: OemHint?): Boolean = hint?.system == "HiOS"
}

/** Everything the status card reads, all from this phone. Counts are null until Room has answered once. */
data class HubInputs(
    val paired: Boolean,
    val notificationAccess: Boolean,
    val listenerConnected: Boolean,
    val listenerDisconnectedAt: Long,
    val listenerConnectedAt: Long,
    val serverContact: String?,
    val online: Boolean,
    val autoSync: Boolean,
    val batteryUnrestricted: Boolean,
    val postNotifications: Boolean,
    val capturedToday: Int?,
    val lastCaptureApp: String?,
    val lastCaptureAt: Long,
    val lastSyncAt: Long,
    /** Notifications not yet sent, typed captures waiting and answers waiting to be sent. */
    val waiting: Int?,
    val oldestWaitingAt: Long?,
    val selfTestPassedAt: Long,
    val oem: OemHint?,
    val now: Long,
    val zone: ZoneId = ZoneId.systemDefault()
)

/** The status card: the calm status, or the one problem that matters most, plus softer notes. */
data class HubStatus(
    val problem: Problem?,
    val softIssues: List<SoftIssue>,
    val todayLine: String,
    val syncOk: Boolean,
    val syncLine: String
) {
    val title: String get() = problem?.title ?: CALM_TITLE

    companion object {
        const val CALM_TITLE = "Lagi jagain notifikasimu"
        const val QUIET_DAY_PASSED = "Tes oke, aku jalan normal. Mungkin emang belum ada transaksi hari ini."

        /** Waiting longer than this with the network up means sending is stuck, not just in progress. */
        const val STUCK_AFTER_MILLIS = 15 * 60 * 1000L

        /** A day with no capture is worth a test only after midday, when a payment is likely by then. */
        const val QUIET_DAY_FROM_HOUR = 12

        private val clock = DateTimeFormatter.ofPattern("HH.mm")

        fun of(input: HubInputs): HubStatus {
            val problem = problem(input)
            return HubStatus(problem, softIssues(input, problem), todayLine(input), syncOk(input), syncLine(input))
        }

        private fun time(millis: Long, zone: ZoneId) = clock.format(Instant.ofEpochMilli(millis).atZone(zone))

        private fun isToday(millis: Long, input: HubInputs): Boolean = millis > 0 &&
            Instant.ofEpochMilli(millis).atZone(input.zone).toLocalDate() == Instant.ofEpochMilli(input.now).atZone(input.zone).toLocalDate()

        /** At most one problem, the one that loses captures first. */
        fun problem(input: HubInputs): Problem? {
            val waiting = input.waiting ?: 0
            if (!input.paired) return Problem(
                ProblemKind.NOT_PAIRED, ProblemTone.BAD, "Belum nyambung ke akunmu",
                "Sambungin dulu biar notifikasi yang ketangkep bisa dicatat.", "Sambungin", FixAction.PAIR
            )
            if (input.serverContact == ServerContact.REFUSED) return Problem(
                ProblemKind.KEY_REFUSED, ProblemTone.BAD, "Sambungan ke akunmu putus",
                "Kuncinya ditolak server, mungkin dicabut dari web. Notifikasi baru tetap aku simpan di HP.",
                "Sambungin ulang", FixAction.PAIR
            )
            if (!input.notificationAccess) return Problem(
                ProblemKind.ACCESS_OFF, ProblemTone.BAD, "Aku nggak bisa denger notifikasi",
                "Akses notifikasinya mati, mungkin abis update HP. Transaksi baru nggak kecatat sampai dinyalain lagi.",
                "Nyalain lagi", FixAction.OPEN_ACCESS
            )
            if (!input.listenerConnected) {
                val since = input.listenerDisconnectedAt.takeIf { it > 0 }?.let { " dari jam ${time(it, input.zone)}" }.orEmpty()
                val saver = OemHints.freezesApps(input.oem) || !input.batteryUnrestricted
                return if (saver) Problem(
                    ProblemKind.LISTENER_ASLEEP, ProblemTone.BAD, "HP-mu nidurin aku",
                    "Penghemat baterai${input.oem?.let { " ${it.system}" }.orEmpty()} ngebekuin aplikasi ini$since. " +
                        "Notifikasi setelah itu bisa kelewat.",
                    "Atur baterai", FixAction.OPEN_BATTERY, input.oem
                ) else Problem(
                    ProblemKind.LISTENER_ASLEEP, ProblemTone.BAD, "Aku lagi putus dari notifikasi",
                    "Pendengarku terputus$since. Notifikasi setelah itu bisa kelewat.",
                    "Sambungin lagi", FixAction.RECONNECT_LISTENER
                )
            }
            if (input.serverContact == ServerContact.UNREACHABLE && input.online) return Problem(
                ProblemKind.SERVER_UNREACHABLE, ProblemTone.WARN, "Server lagi nggak kejangkau",
                if (waiting > 0) "Tenang, $waiting yang nunggu aman di HP. Aku kirim begitu nyambung."
                else "Notifikasi baru tetap aku simpan di HP. Aku kirim begitu nyambung.",
                "Coba lagi", FixAction.RETRY_SERVER
            )
            if (waiting > 0 && !input.online) return Problem(
                ProblemKind.WAITING_TO_SEND, ProblemTone.WARN, "Lagi offline",
                "$waiting nunggu dikirim. Nanti kekirim sendiri pas ada internet.", "Kirim sekarang", FixAction.SEND_NOW
            )
            val stuck = input.oldestWaitingAt?.let { input.now - it >= STUCK_AFTER_MILLIS } == true
            if (waiting > 0 && input.autoSync && stuck) return Problem(
                ProblemKind.WAITING_TO_SEND, ProblemTone.WARN, "Ada $waiting yang belum kekirim",
                "Udah lebih dari 15 menit nunggu. Kirim sekarang, ya.", "Kirim sekarang", FixAction.SEND_NOW
            )
            val hour = Instant.ofEpochMilli(input.now).atZone(input.zone).hour
            if (input.capturedToday == 0 && hour >= QUIET_DAY_FROM_HOUR && !isToday(input.selfTestPassedAt, input)) return Problem(
                ProblemKind.QUIET_DAY, ProblemTone.WARN, "Hari ini masih sepi",
                "Belum ada notifikasi bank atau e-wallet yang ketangkep. Kalau tadi udah bayar, tes aku bentar.",
                "Tes sekarang", FixAction.RUN_SELF_TEST
            )
            return null
        }

        private fun softIssues(input: HubInputs, problem: Problem?): List<SoftIssue> {
            if (!input.paired) return emptyList()
            val issues = mutableListOf<SoftIssue>()
            if (!input.batteryUnrestricted && problem?.fix != FixAction.OPEN_BATTERY) {
                issues += SoftIssue("Belum boleh jalan di latar. HP bisa nidurin aku.", "Izinin", FixAction.OPEN_BATTERY)
            }
            if (!input.postNotifications) {
                issues += SoftIssue("Notifikasi dariku mati. Pertanyaan cuma ada di aplikasi.", "Nyalain", FixAction.OPEN_NOTIFICATION_SETTINGS)
            }
            if (!input.autoSync && problem?.fix != FixAction.SEND_NOW) {
                issues += SoftIssue("Kirim otomatis mati. Kamu yang kirim manual.", "Kirim sekarang", FixAction.SEND_NOW)
            }
            return issues
        }

        /** Today's capture count, never money. */
        fun todayLine(input: HubInputs): String {
            val count = input.capturedToday ?: return "Tangkapan hari ini belum kebaca"
            if (count == 0) return "Belum ada notifikasi yang ketangkep hari ini"
            val last = input.lastCaptureApp?.takeIf { isToday(input.lastCaptureAt, input) }
                ?.let { " · terakhir $it, ${time(input.lastCaptureAt, input.zone)}" }.orEmpty()
            return "$count notifikasi ketangkep hari ini$last"
        }

        private fun syncOk(input: HubInputs): Boolean =
            (input.waiting ?: 0) == 0 && input.serverContact != ServerContact.UNREACHABLE && input.serverContact != ServerContact.REFUSED

        fun syncLine(input: HubInputs): String {
            val waiting = input.waiting
            return when {
                input.serverContact == ServerContact.REFUSED -> "Kunci ditolak server"
                !input.online && (waiting ?: 0) > 0 -> "Offline, $waiting nunggu dikirim"
                input.serverContact == ServerContact.UNREACHABLE -> "Server nggak kejangkau, dicoba lagi otomatis"
                waiting == null -> "Antrean belum kebaca"
                waiting > 0 && !input.autoSync -> "Kirim otomatis mati · $waiting nunggu dikirim"
                waiting > 0 -> "$waiting nunggu dikirim"
                input.lastSyncAt <= 0 -> "Belum ada yang dikirim"
                isToday(input.lastSyncAt, input) -> "Kekirim semua, ${time(input.lastSyncAt, input.zone)}"
                else -> "Kekirim semua"
            }
        }
    }
}
