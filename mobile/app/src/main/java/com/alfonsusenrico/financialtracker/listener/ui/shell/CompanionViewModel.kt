package com.alfonsusenrico.financialtracker.listener.ui.shell

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.alfonsusenrico.financialtracker.listener.data.capture.LabelChange
import com.alfonsusenrico.financialtracker.listener.data.capture.OwnerLabels
import com.alfonsusenrico.financialtracker.listener.data.health.HealthReports
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestOutcome
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestSession
import com.alfonsusenrico.financialtracker.listener.data.health.selfTestRunner
import com.alfonsusenrico.financialtracker.listener.data.prefs.StoredReport
import com.alfonsusenrico.financialtracker.listener.data.report.ReportReadiness
import com.alfonsusenrico.financialtracker.listener.data.report.WebLinks
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResolveRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResultResponse
import com.alfonsusenrico.financialtracker.listener.data.network.serverMessage
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.MappingConfirmation
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeAnswers
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import com.alfonsusenrico.financialtracker.listener.data.sync.SourcePocketQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.SourcePocketReplies
import com.alfonsusenrico.financialtracker.listener.data.sync.SyncScheduler
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.service.PurposeQuestionNotifier
import com.alfonsusenrico.financialtracker.listener.service.SenderConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.service.SourcePocketConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.questions.SourcePocketOptionState
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode
import com.google.gson.Gson
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import retrofit2.Response

/** Every list the feed, the home tiles and Tanya are built from. */
data class CompanionLists(
    val visible: List<RawNotificationEntity> = emptyList(),
    val ignored: List<RawNotificationEntity> = emptyList(),
    val local: List<RawNotificationEntity> = emptyList(),
    val questions: List<RawNotificationEntity> = emptyList(),
    val replies: List<SenderReplyEntity> = emptyList(),
    /** False until Room has answered once, so an empty list here is not yet "none". */
    val loaded: Boolean = false
)

/** A row a notification asked to open: its question in Tanya, or its detail page. */
data class OpenRequest(val rowId: Long, val toQuestions: Boolean)

private val JSON_MEDIA = "application/json; charset=utf-8".toMediaType()

class CompanionViewModel @JvmOverloads constructor(
    application: Application,
    private val db: AppDatabase = AppDatabase.getDatabase(application)
) : AndroidViewModel(application) {

    private val dao = db.notificationDao()
    private val apiClient = ApiClient(application)
    private val ownerLabels = OwnerLabels(dao)
    private val prefs = AppPreferences(application)

    val lists: StateFlow<CompanionLists> = combine(
        dao.getAllNotificationsFlow(), dao.getIgnoredNotificationsFlow(), dao.getLocalOnlyFlow(),
        dao.getNeedsAnswerFlow(), db.senderReplyDao().pendingFlow()
    ) { visible, ignored, local, needsAnswer, replies ->
        CompanionLists(visible, ignored, local, OpenQuestions.open(visible, replies, needsAnswer), replies, loaded = true)
    }.stateIn(viewModelScope, SharingStarted.Eagerly, CompanionLists())

    /** Settled answer attempts, newest first, so Tanya can say a notification reply was not stored. */
    val answerAttempts: StateFlow<List<AnswerAttemptEntity>> =
        db.answerAttemptDao().recentOutcomesFlow().stateIn(viewModelScope, SharingStarted.Eagerly, emptyList())

    val totalCount: StateFlow<Int> = dao.getTotalCountFlow().stateIn(viewModelScope, SharingStarted.Lazily, 0)
    val hiddenDuplicates: StateFlow<Int> =
        dao.getHiddenDuplicateCountFlow().stateIn(viewModelScope, SharingStarted.Lazily, 0)

    private val _readyReport = MutableStateFlow(prefs.readyReport(prefs.pairingContext))
    /** The tracker's ready monthly report for the hub's "Laporan siap" card; null when none is known. */
    val readyReport: StateFlow<StoredReport?> = _readyReport.asStateFlow()

    fun refreshReport() {
        viewModelScope.launch { _readyReport.value = ReportReadiness(getApplication()).refresh() }
    }

    /** The report's full web address, marking it opened so the card steps back; null for a path that is not the tracker's. */
    fun openReport(report: StoredReport): String? {
        prefs.markReportOpened(report.start)
        _readyReport.value = prefs.readyReport(prefs.pairingContext)
        return WebLinks.url(report.url)
    }

    /** The self-test the hub's quiet-day fix and "Tes HP-ku" run. */
    val selfTest = SelfTestSession { selfTestRunner(getApplication()) }

    fun runSelfTest(onDone: (SelfTestOutcome?) -> Unit = {}) {
        viewModelScope.launch { onDone(selfTest.run()) }
    }

    private val _accounts = MutableStateFlow<List<AccountItemResponse>>(emptyList())
    val accounts: StateFlow<List<AccountItemResponse>> = _accounts.asStateFlow()

    private val _categories = MutableStateFlow<List<CategoryItemResponse>>(emptyList())
    val categories: StateFlow<List<CategoryItemResponse>> = _categories.asStateFlow()

    private val _isUpdating = MutableStateFlow(false)
    val isUpdating: StateFlow<Boolean> = _isUpdating.asStateFlow()

    private val _themeMode = MutableStateFlow(ThemeMode.fromCode(prefs.themeMode))
    val themeMode: StateFlow<ThemeMode> = _themeMode.asStateFlow()

    private val _selectedFilter = MutableStateFlow(ActivityFilter.ALL)
    val selectedFilter: StateFlow<ActivityFilter> = _selectedFilter.asStateFlow()

    private val _openRequest = MutableStateFlow<OpenRequest?>(null)
    val openRequest: StateFlow<OpenRequest?> = _openRequest.asStateFlow()

    /** The row shown in Rincian or Tanya; source-pocket options are loaded for it only. */
    private val focusedRowId = MutableStateFlow<Long?>(null)
    private val _sourcePocketOptions = MutableStateFlow(SourcePocketOptionState())
    val sourcePocketOptions: StateFlow<SourcePocketOptionState> = _sourcePocketOptions.asStateFlow()
    private var sourcePocketOptionsJob: Job? = null

    init {
        watchFocusedRow()
        loadMetadata()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun watchFocusedRow() {
        viewModelScope.launch {
            var previous: RawNotificationEntity? = null
            focusedRowId.flatMapLatest { id -> if (id == null) flowOf(null) else dao.observeById(id) }.collect { current ->
                val before = previous
                previous = current
                // A new result, or options cleared by a refresh, means the cached choices may be stale.
                if (current != null && before?.id == current.id && (before.resultJson != current.resultJson ||
                        before.sourcePocketOptionsJson != null && current.sourcePocketOptionsJson == null)
                ) loadSourcePocketOptions(current.id)
            }
        }
    }

    fun row(rowId: Long): Flow<RawNotificationEntity?> = dao.observeById(rowId)

    fun focusRow(rowId: Long?) {
        if (focusedRowId.value == rowId) return
        sourcePocketOptionsJob?.cancel()
        focusedRowId.value = rowId
        _sourcePocketOptions.value = SourcePocketOptionState()
        if (rowId != null) loadSourcePocketOptions(rowId)
    }

    fun setFilter(filter: ActivityFilter) {
        _selectedFilter.value = filter
    }

    fun setThemeMode(mode: ThemeMode) {
        prefs.themeMode = mode.code
        _themeMode.value = mode
    }

    fun requestOpen(rowId: Long) {
        viewModelScope.launch {
            val row = dao.getById(rowId) ?: return@launch
            _openRequest.value = OpenRequest(rowId, OpenQuestions.kindOf(row) != null)
        }
    }

    /** The evening reminder's tap: the question sheet at its first question. */
    fun requestQuestions() {
        _openRequest.value = OpenRequest(-1, toQuestions = true)
    }

    /** A health report now, after the owner fixed something from the hub. */
    fun reportHealthNow() = HealthReports.request(getApplication())

    fun consumeOpenRequest() {
        _openRequest.value = null
    }

    fun resendIgnored(notifId: Long, onComplete: (Boolean) -> Unit) {
        viewModelScope.launch {
            val reset = dao.resubmitIgnored(notifId) == 1
            if (reset) SyncScheduler.enqueueImmediateSync(getApplication())
            onComplete(reset)
        }
    }

    fun sendLocal(notifId: Long, onComplete: (Boolean) -> Unit) {
        viewModelScope.launch {
            val sent = dao.sendLocal(notifId) == 1
            if (sent) SyncScheduler.enqueueImmediateSync(getApplication())
            onComplete(sent)
        }
    }

    /** Hides the row; the callback receives the row as it was so the deletion can be undone. */
    fun deleteFromPhone(notifId: Long, onComplete: (removed: Boolean, previous: RawNotificationEntity?) -> Unit) {
        viewModelScope.launch {
            val row = dao.getById(notifId)
            val removed = dao.dismiss(notifId) == 1
            if (removed) {
                row?.backendEventId?.let {
                    MappingConfirmationNotifier.cancel(getApplication(), it)
                    SenderConfirmationNotifier.cancel(getApplication(), it)
                    SourcePocketConfirmationNotifier.cancel(getApplication(), it)
                }
                row?.recordKey?.takeIf { row.purposeQuestionId != null }?.let {
                    PurposeQuestionNotifier.cancel(getApplication(), it)
                }
            }
            onComplete(removed, row)
        }
    }

    fun undoDelete(previous: RawNotificationEntity) {
        viewModelScope.launch { dao.restoreDismissed(previous.id, previous.processingStatus, previous.nextCheckAt) }
    }

    fun clearIgnored(onComplete: (Int) -> Unit) {
        viewModelScope.launch { onComplete(dao.dismissAllIgnored()) }
    }

    fun loadSourcePocketOptions(rowId: Long) {
        sourcePocketOptionsJob?.cancel()
        sourcePocketOptionsJob = viewModelScope.launch {
            val row = dao.getById(rowId) ?: return@launch
            val item = SourcePocketQuestions.parse(row.resultJson) ?: return@launch
            val questionId = item.confirmation?.questionId ?: return@launch
            val pairing = row.pairingContext ?: return@launch
            if (pairing != prefs.pairingContext || !prefs.isConfigured) {
                _sourcePocketOptions.value = SourcePocketOptionState(rowId, questionId, error = "Hubungkan akun asal notifikasi di Pengaturan.")
                return@launch
            }
            val cache = SourcePocketQuestions.cached(row.sourcePocketOptionsJson, item)
            _sourcePocketOptions.value = SourcePocketOptionState(rowId, questionId, cache?.options.orEmpty(), loading = true, cached = cache != null)
            try {
                val response = apiClient.createService(pairing).getSourcePockets(item.eventId!!)
                if (prefs.pairingContext != pairing || focusedRowId.value != rowId) return@launch
                val options = response.body()?.result
                if (response.isSuccessful && response.body()?.ok == true && options != null && SourcePocketQuestions.validOptions(options, item)) {
                    if (dao.saveSourcePocketOptions(rowId, pairing, item.eventId, Gson().toJson(options), row.resultJson) != 1) return@launch
                    _sourcePocketOptions.value = SourcePocketOptionState(rowId, questionId, options.options.orEmpty())
                } else {
                    if (response.code() in setOf(404, 409)) {
                        if (dao.saveSourcePocketOptions(rowId, pairing, item.eventId, null, row.resultJson) != 1) return@launch
                        SyncScheduler.enqueueCompletion(getApplication(), item.eventId, 0, refresh = true)
                    }
                    _sourcePocketOptions.value = SourcePocketOptionState(rowId, questionId, error = "Pilihan belum tersedia. Segarkan hasil notifikasi.")
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                if (prefs.pairingContext == pairing && focusedRowId.value == rowId) {
                    _sourcePocketOptions.value = SourcePocketOptionState(rowId, questionId, cache?.options.orEmpty(), cached = cache != null,
                        error = if (cache == null) "Koneksi tidak tersedia. Coba lagi untuk memuat kantong." else "Pilihan tersimpan; server akan memeriksa kembali saat terhubung.")
                }
            }
        }
    }

    fun confirmSourcePocket(notifId: Long, questionId: String, accountId: String, onComplete: (Boolean, String?) -> Unit) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                val outcome = SourcePocketReplies(getApplication()).enqueue(notifId, questionId, accountId)
                onComplete(outcome.queued, outcome.message)
            } finally {
                _isUpdating.value = false
            }
        }
    }

    fun confirmMapping(notifId: Long, accountId: String, onComplete: (Boolean, String?) -> Unit) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                val row = dao.getById(notifId)
                val eventId = row?.backendEventId
                if (row?.processingStatus != "needs_confirmation" || eventId == null || !validUuid(accountId)) {
                    onComplete(false, "Notifikasi ini tidak menunggu konfirmasi rekening")
                    return@launch
                }
                val outcome = MappingConfirmation(getApplication()).confirm(eventId, accountId)
                recordAnswer(AnswerAttempts.ACCOUNT, row.id, eventId, OpenQuestions.mappingQuestionId(row), outcome.code)
                onComplete(outcome.success, outcome.message)
            } finally {
                _isUpdating.value = false
            }
        }
    }

    fun confirmSender(notifId: Long, questionId: String, name: String?, onComplete: (Boolean, String?) -> Unit) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                val row = dao.getById(notifId)
                val outcome = if (row?.backendEventId != null && row.pairingContext != null) {
                    SenderReplies(getApplication()).enqueue(row.id, row.backendEventId, questionId, row.pairingContext,
                        if (name == null) "unknown" else "name", name)
                        .also { recordAnswer(AnswerAttempts.SENDER, row.id, row.backendEventId, questionId, it.code) }
                } else SenderReplies.Outcome(false, "Notifikasi tidak ditemukan.")
                onComplete(outcome.queued, outcome.message)
            } finally { _isUpdating.value = false }
        }
    }

    /** Traces an answer given in the app, so Tanya's newest attempt for that question is this one. */
    private suspend fun recordAnswer(kind: String, rowId: Long, eventId: String?, questionId: String?, code: String?) {
        if (code != null) AnswerAttempts.record(db, AnswerAttempts.APP, kind, rowId, eventId, questionId, code)
    }

    fun answerPurpose(notifId: Long, questionId: String, suggestionId: String?, text: String?, onComplete: (Boolean, String?) -> Unit) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                val outcome = PurposeAnswers(getApplication(), db).enqueue(notifId, questionId, suggestionId, text)
                recordAnswer(AnswerAttempts.PURPOSE, notifId, dao.getById(notifId)?.backendEventId, questionId, outcome.code)
                onComplete(outcome.queued, outcome.message)
            } finally {
                _isUpdating.value = false
            }
        }
    }

    fun loadMetadata() {
        // Without credentials the tracker would only refuse the request.
        if (!prefs.isConfigured) return
        viewModelScope.launch {
            try {
                val service = apiClient.createService()
                val accRes = service.getAccounts()
                if (accRes.isSuccessful) {
                    _accounts.value = accRes.body()?.accounts ?: emptyList()
                }
                val catRes = service.getCategories()
                if (catRes.isSuccessful) {
                    _categories.value = catRes.body()?.categories ?: emptyList()
                }
            } catch (_: Exception) {
                // Offline: the pickers stay empty and say so.
            }
        }
    }

    /**
     * "Perbaiki label" on a recorded notification: category, pillar and merchant only, through the owner-edit
     * path, so the tracker stores the correction as owner feedback. The row's result is then read again.
     */
    fun correctLabel(notifId: Long, change: LabelChange, onComplete: (Boolean, String?) -> Unit) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                val stored = dao.getById(notifId)
                val item = stored?.let { RowPresentation.of(it) }
                val transactionId = stored?.editableTransactionId
                if (stored == null || item == null || stored.processingStatus != "recorded" || stored.processingError != null ||
                    !validUuid(transactionId) || stored.pairingContext != prefs.pairingContext
                ) {
                    onComplete(false, "Label transaksi ini hanya bisa diubah di web.")
                    return@launch
                }
                change.problem(item.recorded?.type)?.let {
                    onComplete(false, it)
                    return@launch
                }
                val response = apiClient.createService(prefs.pairingContext)
                    .correctLabel(transactionId!!, change.toJson().toRequestBody(JSON_MEDIA))
                if (response.isSuccessful && response.body()?.ok != false) {
                    stored.backendEventId?.let { SyncScheduler.enqueueCompletion(getApplication(), it, 0, refresh = true) }
                    onComplete(true, null)
                } else {
                    onComplete(false, when (response.code()) {
                        401, 403 -> "Kunci ditolak tracker. Sambungin ulang di Pengaturan."
                        404 -> "Transaksinya udah nggak ada di tracker."
                        409 -> "Transaksi ini terhubung dengan transaksi lain. Ubah lewat web."
                        422 -> "Tracker menolak label ini. Coba pilih lagi."
                        else -> "Tracker belum bisa dihubungi. Coba lagi nanti."
                    })
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                onComplete(false, "Koneksi nggak tersedia. Labelnya belum berubah.")
            } finally {
                _isUpdating.value = false
            }
        }
    }

    fun saveAndSyncUnsynced(
        notifId: Long,
        amount: Long,
        type: String,
        notes: String?,
        onComplete: (Boolean, String?) -> Unit
    ) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                if (!ownerLabels.labelForSend(notifId, amount, type, notes)) {
                    onComplete(false, "Notifikasi sudah dikirim; ubah label hanya di riwayat lokal")
                    return@launch
                }
                SyncScheduler.enqueueImmediateSync(getApplication())
                onComplete(true, null)
            } catch (_: Exception) {
                onComplete(false, "Gagal menyimpan label")
            } finally {
                _isUpdating.value = false
            }
        }
    }

    fun retryNotification(notifId: Long, onComplete: (Boolean, String?) -> Unit) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                val row = unresolvedRow(notifId)
                if (row == null) {
                    onComplete(false, "Notifikasi ini tidak dapat diulang")
                    return@launch
                }
                val response = apiClient.createService().retryNotification(row.backendEventId!!)
                applyServerResult(row, response, onComplete)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                onComplete(false, "Koneksi tidak tersedia")
            } finally {
                _isUpdating.value = false
            }
        }
    }

    fun resolveNotification(
        notifId: Long,
        amount: Long,
        type: String,
        accountId: String,
        categoryId: String,
        notes: String?,
        onComplete: (Boolean, String?) -> Unit
    ) {
        viewModelScope.launch {
            _isUpdating.value = true
            try {
                val row = unresolvedRow(notifId)
                if (row == null || type !in setOf("expense", "income")) {
                    onComplete(false, "Notifikasi ini tidak dapat dicatat manual")
                    return@launch
                }
                val response = apiClient.createService().resolveNotification(
                    row.backendEventId!!,
                    NotificationResolveRequest(type, accountId, categoryId, amount, notes)
                )
                applyServerResult(row, response, onComplete)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                onComplete(false, "Koneksi tidak tersedia")
            } finally {
                _isUpdating.value = false
            }
        }
    }

    private suspend fun unresolvedRow(notifId: Long): RawNotificationEntity? {
        val row = dao.getById(notifId) ?: return null
        val unresolved = row.processingStatus in setOf("needs_review", "failed")
        return row.takeIf { it.isSynced && it.pairingContext == prefs.pairingContext && validUuid(it.backendEventId) && unresolved }
    }

    private suspend fun applyServerResult(
        row: RawNotificationEntity,
        response: Response<NotificationResultResponse>,
        onComplete: (Boolean, String?) -> Unit
    ) {
        val item = response.body()?.result
        if (!response.isSuccessful || response.body()?.ok != true || item == null || item.eventId != row.backendEventId) {
            onComplete(false, serverMessage(response))
            return
        }
        val pairingContext = prefs.pairingContext
        val results = NotificationResults(getApplication())
        if (!results.consume(row, item, pairingContext)) {
            onComplete(false, "Balasan server tidak valid")
            return
        }
        results.deliverPendingAlerts()
        val saved = dao.getById(row.id)
        if (saved?.backendEventId != null && saved.processingStatus in setOf("queued", "processing")) {
            SyncScheduler.enqueueCompletion(
                getApplication(), saved.backendEventId, (saved.nextCheckAt ?: 0L) - System.currentTimeMillis(), true
            )
        }
        onComplete(true, null)
    }

    fun syncNow() {
        SyncScheduler.enqueueImmediateSync(getApplication())
        // The owner's own request, so queued typed captures go too whatever the auto-sync switch says.
        SyncScheduler.enqueueTextCaptures(getApplication())
        viewModelScope.launch {
            val now = System.currentTimeMillis()
            val pairingContext = prefs.pairingContext
            for (row in dao.getRefreshableResults(pairingContext)) {
                row.backendEventId?.let { SyncScheduler.enqueueCompletion(getApplication(), it, 0, refresh = true) }
            }
            for (row in dao.getDueCompletion(pairingContext, now)) {
                row.backendEventId?.let { SyncScheduler.enqueueCompletion(getApplication(), it, 0) }
            }
        }
    }
}
