package com.alfonsusenrico.financialtracker.listener.data.report

import android.content.Context
import com.alfonsusenrico.financialtracker.listener.BuildConfig
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.model.LatestReportResponse
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.prefs.StoredReport
import kotlinx.coroutines.CancellationException
import java.time.LocalDate
import java.time.format.DateTimeParseException

/** Web addresses on the tracker this build talks to; only the tracker's own paths are opened. */
object WebLinks {
    const val REPORT = "/laporan"
    const val LEDGER = "/ledger"
    const val REVIEW = "/review"
    const val SETTINGS = "/settings"

    /** A tracker path ("/laporan?basis=month&start=2026-10-01") as a full URL, or null for anything else. */
    fun url(path: String, server: String = BuildConfig.SERVER_URL): String? {
        if (!path.startsWith("/") || path.startsWith("//") || path.contains('\\') || path.any { it.isWhitespace() }) return null
        return server.trimEnd('/') + path
    }
}

/**
 * "Laporan siap": the tracker says the latest closed period's monthly report is ready
 * (`GET /api/reports/monthly/latest`). The phone keeps only its label, start date and web path, shows a card
 * on the hub, and notifies once per period; the report itself is read on the web.
 */
class ReportReadiness(
    private val context: Context,
    private val prefs: AppPreferences = AppPreferences(context),
    private val notify: (StoredReport) -> Boolean = { ReportNotifier.post(context, it) },
    private val now: () -> Long = System::currentTimeMillis
) {
    suspend fun refresh(): StoredReport? {
        if (!prefs.isConfigured) return null
        val pairing = prefs.pairingContext
        val response = try {
            ApiClient(context).createService(pairing).latestMonthlyReport()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return prefs.readyReport(pairing)
        }
        prefs.reportCheckedAt = now()
        // An older tracker without the endpoint (404) or any other failure leaves what was known.
        if (!response.isSuccessful) return prefs.readyReport(pairing)
        val ready = valid(response.body())
        if (ready == null) {
            if (response.body()?.ok == true && response.body()?.ready == false) prefs.clearReadyReport()
            return prefs.readyReport(pairing)
        }
        if (!prefs.saveReadyReport(pairing, ready.label, ready.url, ready.start)) return null
        val stored = prefs.readyReport(pairing) ?: return null
        if (prefs.reportNotifiedStart != stored.start && !stored.opened && notify(stored)) prefs.reportNotifiedStart = stored.start
        return stored
    }

    companion object {
        /** Checked on open and at most this often from the background. */
        const val CHECK_INTERVAL_MILLIS = 6 * 60 * 60 * 1000L

        /** The report the tracker says is ready, or null when it is not, or the answer breaks the contract. */
        fun valid(body: LatestReportResponse?): StoredReport? {
            if (body?.ok != true || body.ready != true) return null
            val label = body.period?.label?.trim()?.takeIf { it.isNotEmpty() && it.length <= 60 } ?: return null
            val start = body.period.startDate?.takeIf {
                try {
                    LocalDate.parse(it)
                    true
                } catch (_: DateTimeParseException) {
                    false
                }
            } ?: return null
            val url = body.url?.takeIf { it.startsWith(WebLinks.REPORT) && WebLinks.url(it) != null } ?: return null
            return StoredReport(label, url, start, opened = false)
        }

        suspend fun refreshIfDue(context: Context) {
            val prefs = AppPreferences(context)
            if (System.currentTimeMillis() - prefs.reportCheckedAt < CHECK_INTERVAL_MILLIS) return
            ReportReadiness(context, prefs).refresh()
        }
    }
}
