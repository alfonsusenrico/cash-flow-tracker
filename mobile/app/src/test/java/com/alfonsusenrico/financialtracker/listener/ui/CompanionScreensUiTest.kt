package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertIsEnabled
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.assertIsSelected
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasScrollToNodeAction
import androidx.compose.ui.test.performScrollToNode
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.ui.components.DELETE_ACTION_LABEL
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityScreen
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailActions
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.questions.QuestionAnswers
import com.alfonsusenrico.financialtracker.listener.ui.questions.QuestionsScreen
import com.alfonsusenrico.financialtracker.listener.ui.questions.SourcePocketOptionState
import com.alfonsusenrico.financialtracker.listener.ui.settings.AppToggle
import com.alfonsusenrico.financialtracker.listener.ui.settings.NO_FINANCE_APPS
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsActions
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsScreen
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsState
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountChoice
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode
import androidx.compose.material3.Text
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config

/** Each screen and its states, in the light and the dark theme. */
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class CompanionScreensUiTest(private val dark: Boolean) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0}")
        fun themes() = listOf(arrayOf<Any>(false), arrayOf<Any>(true))
    }

    @get:Rule val compose = createComposeRule()

    private val lists = fixtureLists()
    private val heading = SemanticsMatcher.keyIsDefined(SemanticsProperties.Heading)

    // ---- Aktivitas ----

    @Test fun activityListsEveryRowWithFiltersAndTheDuplicatesNote() {
        val chosen = mutableListOf<ActivityFilter>()
        compose.show(dark) {
            ActivityScreen(lists, ActivityFilter.ALL, totalCount = lists.visible.size, hiddenDuplicates = 2, today = UiFixtures.today,
                onFilter = { chosen += it }, onOpenRow = { _, _ -> }, onDelete = {}, onClearIgnored = {})
        }
        compose.onNodeWithText("9 notifikasi · 2 kembar disembunyikan · nominal per item, tanpa total").assertExists()
        // Each item keeps its own amount; no day shows a total of money out.
        compose.onNodeWithText("Keluar Rp", substring = true).assertDoesNotExist()
        compose.onNode(hasContentDescription("Semua") and SemanticsMatcher.keyIsDefined(SemanticsProperties.Selected)).assertIsSelected()
        compose.onNodeWithContentDescription("Perlu dijawab, 4").assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithContentDescription("Perlu cek, 5").performClick()
        compose.onNodeWithContentDescription("Di ponsel, 1").performScrollTo().performClick()
        compose.onNodeWithContentDescription("Diabaikan, 1").performScrollTo().performClick()
        compose.onNodeWithContentDescription("Stockbit").performScrollTo().performClick()
        assertEquals(
            listOf(ActivityFilter.NEEDS_ANSWER, ActivityFilter.PENDING, ActivityFilter.LOCAL, ActivityFilter.IGNORED, ActivityFilter.STOCKBIT),
            chosen
        )
        compose.onNodeWithContentDescription("Kopi Contoh, Keluar Rp32.000, Tercatat", substring = true).assertExists()
        val feed = compose.onNode(hasScrollToNodeAction() and SemanticsMatcher.keyIsDefined(SemanticsProperties.VerticalScrollAxisRange))
        feed.performScrollToNode(hasContentDescription("Pesanan Contoh, Nominal belum jelas, Perlu cek", substring = true))
        feed.performScrollToNode(hasText("2 notifikasi kembar disembunyikan", substring = true))
        compose.onNodeWithText("2 notifikasi kembar disembunyikan", substring = true).assertIsDisplayed()
    }

    @Test fun feedRowsOpenTheirQuestionOrDetailAndOfferDelete() {
        val opened = mutableListOf<Pair<Long, Boolean>>()
        val deleted = mutableListOf<Long>()
        compose.show(dark) {
            ActivityScreen(lists, ActivityFilter.ALL, 9, 0, UiFixtures.today, {}, onOpenRow = { id, q -> opened += id to q },
                onDelete = { deleted += it }, onClearIgnored = {})
        }
        compose.onNodeWithContentDescription("Toko Contoh Abadi", substring = true).performClick()
        compose.onNodeWithContentDescription("Kopi Contoh", substring = true).performClick()
        assertEquals(listOf(UiFixtures.purpose().id to true, 20L to false), opened)
        val row = compose.onNodeWithContentDescription("Kopi Contoh", substring = true).fetchSemanticsNode()
        val delete = row.config[SemanticsActions.CustomActions].single { it.label == DELETE_ACTION_LABEL }
        compose.runOnIdle { delete.action() }
        assertEquals(listOf(20L), deleted)
    }

    @Test fun ignoredItemsAreClearedOnlyAfterConfirmation() {
        var cleared = 0
        compose.show(dark) {
            ActivityScreen(lists, ActivityFilter.IGNORED, 9, 0, UiFixtures.today, {}, { _, _ -> }, {}, onClearIgnored = { cleared++ })
        }
        compose.onNodeWithText("Ditolak server karena bukan transaksi", substring = true).assertExists()
        compose.onNodeWithText("Hapus semua").performClick()
        compose.onNodeWithText("Hapus semua yang diabaikan?").assertIsDisplayed()
        compose.onNodeWithText("Batal").performClick()
        assertEquals(0, cleared)
        compose.onNodeWithText("Hapus semua").performClick()
        compose.onNodeWithText("Hapus Semua").performClick()
        assertEquals(1, cleared)
    }

    @Test fun emptyFilterSaysSo() {
        compose.show(dark) {
            ActivityScreen(lists.copy(local = emptyList()), ActivityFilter.LOCAL, 9, 0, UiFixtures.today, {}, { _, _ -> }, {}, {})
        }
        compose.onNodeWithText("Kosong untuk saringan ini").assertExists()
    }

    // ---- Rincian ----

    private class Calls {
        val log = mutableListOf<String>()
    }

    private fun detail(row: RawNotificationEntity?, calls: Calls = Calls()): Calls {
        compose.show(dark) {
            DetailScreen(
                row, emptyList(), UiFixtures.PAIRING, emptyList(), emptyList(), busy = false, today = UiFixtures.today,
                actions = DetailActions(
                    onBack = { calls.log += "back" }, onDelete = { calls.log += "delete:$it" },
                    onOpenQuestion = { calls.log += "question:$it" }, onRetry = { calls.log += "retry:$it" },
                    onSendLocal = { calls.log += "send:$it" }, onResend = { calls.log += "resend:$it" },
                    onInvalid = { calls.log += "invalid:$it" },
                    onSubmit = { r, mode, editable, draft, done -> calls.log += "submit:${r.id}:$mode:$editable:${draft.amount}"; done(true) },
                    onCorrectLabel = { r, change, done ->
                        calls.log += "label:${r.id}:" + listOfNotNull(change.categoryId, change.pillar?.let { it::class.simpleName }, change.merchant).joinToString(":")
                        done(true)
                    },
                    onOpenLedger = { calls.log += "ledger" }
                )
            )
        }
        return calls
    }

    @Test fun reviewDetailOffersRetryAndManualRecord() {
        val calls = detail(UiFixtures.review())
        compose.onNode(hasText("Rincian") and heading).assertExists()
        compose.onNodeWithText("Perlu dicek").assertExists()
        compose.onNodeWithText("Coba Proses Ulang").performClick()
        compose.onNodeWithText("Catat manual").performClick()
        compose.onNode(hasText("Catat manual") and heading).assertExists()
        compose.onNodeWithText("Catat ke Tracker").performScrollTo()
        compose.onNodeWithText("Notifikasi asli").performScrollTo()
        assertEquals(listOf("retry:${UiFixtures.review().id}"), calls.log)
    }

    @Test fun phoneIgnoredAndQuestionDetailsOfferTheirAction() {
        val row = androidx.compose.runtime.mutableStateOf<RawNotificationEntity?>(UiFixtures.phone())
        val calls = Calls()
        compose.show(dark) {
            DetailScreen(row.value, emptyList(), UiFixtures.PAIRING, emptyList(), emptyList(), false, UiFixtures.today,
                DetailActions({}, {}, { calls.log += "question:$it" }, {}, { calls.log += "send:$it" }, { calls.log += "resend:$it" }, {},
                    { _, _, _, _, _ -> }))
        }
        compose.onNodeWithText("Disimpan di ponsel").assertExists()
        compose.onNodeWithText("Kirim ke Tracker").performClick()
        row.value = UiFixtures.ignored()
        compose.onNodeWithText("Diabaikan server").assertExists()
        compose.onNodeWithText("Kirim Ulang").performClick()
        row.value = UiFixtures.sender()
        compose.onNode(hasText("Siapa pengirimnya?") and heading).assertExists()
        compose.onNodeWithText("Jawab sekarang").performClick()
        assertEquals(listOf("send:8", "resend:9", "question:4"), calls.log)
    }

    @Test fun recordedDetailCorrectsTheLabelAndLinksTheFullEditToTheWeb() {
        val recorded = UiFixtures.recorded(20, "Kopi Contoh", 32_000)
        val calls = detail(recorded)
        compose.onNodeWithText("Yang dicatat tracker").assertExists()
        compose.onNodeWithText("Makan & Minum").assertExists()
        compose.onNodeWithText("Ubah transaksi").assertDoesNotExist()
        compose.onNodeWithText("Lihat di Buku Kas (web)").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("Perbaiki label").performScrollTo().performClick()
        compose.onNode(hasText("Perbaiki label") and heading).assertExists()
        compose.onNodeWithText("Sekarang: Makan & Minum").assertExists()
        compose.onNodeWithText("Belum diklasifikasi").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("Simpan label").performScrollTo().performClick()
        assertEquals(listOf("ledger", "label:20:Unclassified"), calls.log)
    }

    @Test fun labelCorrectionWithNothingChosenIsNotSent() {
        val calls = detail(UiFixtures.recorded(20, "Kopi Contoh", 32_000))
        compose.onNodeWithText("Perbaiki label").performScrollTo().performClick()
        compose.onNodeWithText("Simpan label").performScrollTo().performClick()
        compose.onNodeWithText("Pilih kategori, pilar, atau isi merchant dulu.").assertExists()
        assertEquals(emptyList<String>(), calls.log)
    }

    @Test fun movementCannotBeEditedOnThePhone() {
        detail(UiFixtures.movement())
        compose.onNodeWithText("Pindah saldo (tidak dihitung pengeluaran)").assertExists()
        compose.onNodeWithText("Perbaiki label").assertDoesNotExist()
        compose.onNodeWithText("Lihat di Buku Kas (web)").performScrollTo().assertIsEnabled()
    }

    @Test fun deleteFromDetailNeedsConfirmation() {
        val calls = detail(UiFixtures.review())
        compose.onNodeWithText("Hapus dari aplikasi").performScrollTo().performClick()
        compose.onNodeWithText("Hapus dari aplikasi?").assertIsDisplayed()
        compose.onNodeWithText("Batal").performClick()
        assertTrue(calls.log.isEmpty())
        compose.onNodeWithContentDescription("Hapus dari aplikasi").performClick()
        compose.onNodeWithText("Hapus").performClick()
        assertEquals(listOf("delete:${UiFixtures.review().id}"), calls.log)
    }

    @Test fun missingRowSaysItWasDeleted() {
        detail(null)
        compose.onNodeWithText("Item tidak ditemukan").assertExists()
    }

    // ---- Tanya ----

    private fun tanya(
        row: RawNotificationEntity?, questions: List<RawNotificationEntity> = lists.questions,
        pocket: SourcePocketOptionState = SourcePocketOptionState(), attempts: List<AnswerAttemptEntity> = emptyList()
    ): MutableList<String> {
        val log = mutableListOf<String>()
        compose.show(dark) {
            QuestionsScreen(
                row, questions, emptyList(), UiFixtures.PAIRING, emptyList(), pocket, busy = false, today = UiFixtures.today,
                onShow = { log += "show:$it" }, onOpenDetail = { log += "detail:$it" }, onHome = { log += "home" },
                answers = QuestionAnswers(
                    purpose = { id, _, s, t -> log += "purpose:$id:$s:$t" },
                    sender = { id, _, name -> log += "sender:$id:$name" },
                    sourcePocket = { id, _, account -> log += "pocket:$id:$account" },
                    refreshPockets = { log += "refresh:$it" },
                    mapping = { id, account -> log += "mapping:$id:$account" }
                ),
                attempts = attempts
            )
        }
        return log
    }

    @Test fun purposeQuestionShowsTheOriginalNotificationFirstAndPages() {
        val purpose = UiFixtures.purpose()
        val log = tanya(purpose)
        compose.onNodeWithText("1 dari 4 menunggu").assertExists()
        val quote = compose.onNodeWithText("Transaksi QRIS Rp187.500,00", substring = true).fetchSemanticsNode().boundsInRoot
        val title = compose.onNode(hasText("Apa ini?") and heading).fetchSemanticsNode().boundsInRoot
        assertTrue("The notification text comes before the question", quote.bottom <= title.top)
        compose.onNodeWithContentDescription("Notifikasi asli dari myBCA", substring = true).assertExists()
        compose.onNodeWithContentDescription("Jawab: Belanja harian, kategori Belanja Harian").performScrollTo().performClick()
        compose.onNodeWithContentDescription("Pertanyaan sebelumnya").performScrollTo().assertIsNotEnabled()
        compose.onNodeWithContentDescription("Pertanyaan berikutnya").performClick()
        assertEquals(listOf("purpose:${purpose.id}:s1:null", "show:${lists.questions[1].id}"), log)
    }

    @Test fun senderQuestionTakesANameOrNone() {
        val sender = UiFixtures.sender()
        val log = tanya(sender)
        compose.onNode(hasText("Siapa pengirimnya?") and heading).assertExists()
        compose.onNodeWithContentDescription("Nama pengirim tersamar: ***LA ****NI").assertExists()
        compose.onNodeWithText("Nama pengirim").performScrollTo().performTextInput("Nama Contoh")
        compose.onNodeWithText("Konfirmasi nama").performScrollTo().performClick()
        compose.onNodeWithText("Catat tanpa nama").performScrollTo().performClick()
        assertEquals(listOf("sender:${sender.id}:Nama Contoh", "sender:${sender.id}:null"), log)
    }

    private fun attempt(
        id: Long, channel: String, outcome: String, kind: String = AnswerAttempts.SENDER, row: Long = 4,
        event: String = UiFixtures.uuid(4), question: String? = UiFixtures.uuid(904)
    ) = AnswerAttemptEntity(
        id = id, createdAt = id, channel = channel, answerKind = kind, rowId = row, eventId = event,
        questionId = question, outcome = outcome, appVersion = "fictional"
    )

    @Test fun purposeQuestionSaysANotificationAnswerWasNotSaved() {
        val purpose = UiFixtures.purpose()
        val log = tanya(purpose, attempts = listOf(
            attempt(2, AnswerAttempts.NOTIFICATION, AnswerAttempts.LOCKED, AnswerAttempts.PURPOSE, 1, UiFixtures.uuid(1), UiFixtures.uuid(901))
        ))
        compose.onNodeWithText("Jawaban dari notifikasi belum tersimpan karena ponsel terkunci. Jawab di sini.")
            .performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Jawab: Belanja harian, kategori Belanja Harian").performScrollTo().performClick()
        assertEquals(listOf("purpose:${purpose.id}:s1:null"), log)
    }

    @Test fun mappingQuestionSaysANotificationConfirmationWasNotSaved() {
        tanya(UiFixtures.mapping(), attempts = listOf(
            attempt(2, AnswerAttempts.NOTIFICATION, AnswerAttempts.SERVER_REFUSED, AnswerAttempts.ACCOUNT, 6, UiFixtures.uuid(6), null)
        ))
        compose.onNodeWithText("Konfirmasi dari notifikasi belum tersimpan. Jawab di sini.").performScrollTo().assertIsDisplayed()
    }

    @Test fun senderQuestionSaysANotificationReplyWasNotSaved() {
        val sender = UiFixtures.sender()
        val log = tanya(sender, attempts = listOf(attempt(2, AnswerAttempts.NOTIFICATION, AnswerAttempts.LOCKED)))
        compose.onNodeWithText("Balasan dari notifikasi belum tersimpan karena ponsel terkunci. Jawab di sini.")
            .performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Nama pengirim").performScrollTo().performTextInput("Nama Contoh")
        compose.onNodeWithText("Konfirmasi nama").performScrollTo().performClick()
        assertEquals(listOf("sender:${sender.id}:Nama Contoh"), log)
    }

    @Test fun senderQuestionDropsTheNoticeOnceANewerAttemptSettles() {
        tanya(
            UiFixtures.sender(),
            attempts = listOf(attempt(3, AnswerAttempts.APP, AnswerAttempts.INVALID_NAME), attempt(2, AnswerAttempts.NOTIFICATION, AnswerAttempts.TIMEOUT))
        )
        compose.onNodeWithText("Balasan dari notifikasi belum tersimpan", substring = true).assertDoesNotExist()
        compose.onNodeWithText("Konfirmasi nama").assertExists()
    }

    @Test fun pocketQuestionNeedsAnExplicitChoice() {
        val pocket = UiFixtures.pocket()
        val options = SourcePocketOptionState(pocket.id, UiFixtures.uuid(905),
            listOf(AccountChoice(UiFixtures.uuid(715), "Bank Jago · Kantong Utama"), AccountChoice(UiFixtures.uuid(725), "Bank Jago · Dana Contoh")))
        val log = tanya(pocket, pocket = options)
        compose.onNode(hasText("Dari rekening atau kantong mana?") and heading).assertExists()
        compose.onNodeWithText("Konfirmasi sumber dana").performScrollTo().assertIsNotEnabled()
        compose.onNodeWithText("Bank Jago · Dana Contoh").performScrollTo().performClick().assertIsSelected()
        compose.onNodeWithText("Konfirmasi sumber dana").performScrollTo().assertIsEnabled().performClick()
        compose.onNodeWithText("Segarkan pilihan").performScrollTo().performClick()
        assertEquals(listOf("pocket:${pocket.id}:${UiFixtures.uuid(725)}", "refresh:${pocket.id}"), log)
    }

    @Test fun pocketQuestionPreselectsTheSuggestionButWaitsForTheOwner() {
        val pocket = UiFixtures.pocket(suggested = UiFixtures.uuid(725))
        val options = SourcePocketOptionState(pocket.id, UiFixtures.uuid(905),
            listOf(AccountChoice(UiFixtures.uuid(715), "Bank Jago · Kantong Utama"), AccountChoice(UiFixtures.uuid(725), "Bank Jago · Dana Contoh")))
        val log = tanya(pocket, pocket = options)
        compose.onNodeWithText("Bank Jago · Dana Contoh").performScrollTo().assertIsSelected()
        compose.onNodeWithText("Disarankan").assertExists()
        assertEquals(emptyList<String>(), log)
        compose.onNodeWithText("Konfirmasi sumber dana").performScrollTo().assertIsEnabled().performClick()
        assertEquals(listOf("pocket:${pocket.id}:${UiFixtures.uuid(725)}"), log)
    }

    @Test fun mappingQuestionConfirmsTheProposedAccount() {
        val mapping = UiFixtures.mapping()
        val log = tanya(mapping)
        compose.onNode(hasText("Rekening yang mana?") and heading).assertExists()
        compose.onNodeWithText("yakin 72%", substring = true).assertExists()
        compose.onNodeWithText("BCA Tabungan Contoh").performScrollTo().assertIsSelected()
        compose.onNodeWithText("Konfirmasi Rekening").performScrollTo().performClick()
        assertEquals(listOf("mapping:${mapping.id}:${UiFixtures.uuid(706)}"), log)
    }

    @Test fun answeredQuestionStaysWithItsStateUntilTheOwnerMovesOn() {
        val sender = UiFixtures.sender()
        val log = tanya(sender, questions = lists.questions.filter { it.id != sender.id })
        compose.onNodeWithText("Berikutnya · 3 lagi").performScrollTo().performClick()
        compose.onNodeWithContentDescription("Pertanyaan berikutnya").assertDoesNotExist()
        assertEquals(listOf("show:${lists.questions.first { it.id != sender.id }.id}"), log)
    }

    @Test fun closedQuestionPointsToTheTransaction() {
        val settled = UiFixtures.recorded(20, "Kopi Contoh", 32_000)
        val log = tanya(settled, questions = emptyList())
        compose.onNodeWithText("Ditutup").assertExists()
        compose.onNodeWithText("Lihat transaksi").performScrollTo().performClick()
        compose.onNodeWithText("Kembali ke beranda").performScrollTo().performClick()
        assertEquals(listOf("detail:20", "home"), log)
    }

    @Test fun noQuestionsLeftSaysAllAnswered() {
        tanya(null, questions = emptyList())
        compose.onNodeWithText("Semua terjawab").assertExists()
        compose.onNodeWithText("Tidak ada yang menunggu").assertExists()
    }

    // ---- Atur ----

    @Test fun settingsSayWhenNoFinanceAppIsInstalled() {
        compose.show(dark) {
            SettingsScreen(
                SettingsState(autoSync = true, apps = emptyList(), listenerPermissionGranted = true, batteryExempt = true, themeMode = ThemeMode.SYSTEM),
                SettingsActions({}, {}, { _, _ -> }, {}, {}, {}, {}),
                connection = {}
            )
        }
        compose.onNodeWithText(NO_FINANCE_APPS).performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Cuma dari aplikasi ini", substring = true).assertDoesNotExist()
        compose.onNodeWithText("Dipantau").assertDoesNotExist()
    }

    @Test fun settingsAreGroupedListsWithTheEveningReminder() {
        val log = mutableListOf<String>()
        compose.show(dark) {
            SettingsScreen(
                SettingsState(
                    autoSync = true,
                    apps = listOf(AppToggle("com.gojek.gopay", "GoPay", true), AppToggle("com.bca", "BCA mobile", false)),
                    listenerPermissionGranted = true, batteryExempt = false, themeMode = ThemeMode.SYSTEM,
                    reminderEnabled = true, reminderMinute = 20 * 60, version = "2.4.0"
                ),
                SettingsActions(
                    onAutoSync = { log += "auto:$it" }, onSyncNow = { log += "sync" },
                    onToggleApp = { p, on -> log += "app:$p:$on" }, onOpenListenerAccess = { log += "listener" },
                    onOpenBattery = { log += "battery" }, onOpenAppInfo = { log += "info" }, onThemeMode = { log += "theme:$it" },
                    onReminder = { log += "reminder:$it" }, onReminderTime = { log += "time" }, onSelfTest = { log += "test" }
                ),
                connection = { Text("Koneksi") }
            )
        }
        listOf("Aplikasi yang dipantau", "Kirim", "Pengingat malam", "Izin & baterai", "Tampilan", "Tentang").forEach {
            compose.onNode(hasText(it) and heading).performScrollTo().assertExists()
        }
        // The money figures and health tiles of 2.3 are gone from settings.
        compose.onNodeWithText("Pendengar notifikasi".uppercase()).assertDoesNotExist()
        compose.onNodeWithText("Kirim otomatis").performScrollTo().performClick()
        compose.onNodeWithText("Sinkron sekarang").performScrollTo().performClick()
        compose.onNodeWithText("BCA mobile").performScrollTo().performClick()
        compose.onNodeWithText("Satu pengingat jam 20.00").performScrollTo().assertExists()
        compose.onNodeWithText("Pertanyaan ringan nunggu malam").performScrollTo().performClick()
        compose.onNodeWithText("Jam pengingat").performScrollTo().performClick()
        compose.onNodeWithText("Optimasi baterai").performScrollTo().performClick()
        compose.onNodeWithText("Phone Master", substring = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Tes HP-ku").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("Ikuti sistem").performScrollTo().assertIsSelected()
        compose.onNodeWithText("Gelap").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("versi 2.4.0", substring = true).performScrollTo().assertExists()
        assertEquals(
            listOf("auto:false", "sync", "app:com.bca:true", "reminder:false", "time", "battery", "test", "theme:${ThemeMode.DARK}"),
            log
        )
    }
}
