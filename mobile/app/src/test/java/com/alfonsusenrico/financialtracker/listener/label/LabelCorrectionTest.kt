package com.alfonsusenrico.financialtracker.listener.label

import android.app.Application
import androidx.lifecycle.viewModelScope
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.work.Configuration
import androidx.work.testing.SynchronousExecutor
import androidx.work.testing.WorkManagerTestInitHelper
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.TestEndpoint
import com.alfonsusenrico.financialtracker.listener.data.capture.LabelChange
import com.alfonsusenrico.financialtracker.listener.data.capture.PillarChoice
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.UiFixtures
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.google.gson.JsonParser
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import android.os.Looper
import org.robolectric.Shadows.shadowOf
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.RecordedRequest
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** "Perbaiki label": only what the owner changed goes to the owner-edit path, with an explicit null to clear the pillar. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class LabelCorrectionTest {
    private val app = ApplicationProvider.getApplicationContext<Application>()
    private val category = "00000000-0000-4000-8000-000000000204"
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private lateinit var model: CompanionViewModel

    private val patches = java.util.Collections.synchronizedList(mutableListOf<RecordedRequest>())
    @Volatile private var patchReply = MockResponse().setBody("""{"ok":true}""")

    @Before fun open() {
        WorkManagerTestInitHelper.initializeTestWorkManager(app, Configuration.Builder().setExecutor(SynchronousExecutor()).build())
        server = MockWebServer()
        // The view model also reads accounts and categories when it starts; only the label patch is answered here.
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse =
                if (request.method == "PATCH") patchReply.also { patches += request } else MockResponse().setResponseCode(404)
        }
        server.start()
        TestEndpoint.set(app, server.url("/").toString())
        AppPreferences(app).apiKey = "cfk_synthetic_label_key_0001"
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).build()
        model = CompanionViewModel(app, db)
    }

    @After fun close() {
        model.viewModelScope.cancel()
        db.close()
        server.shutdown()
        AppPreferences(app).apiKey = ""
    }

    @Test fun onlyTheChangedFieldsAreSentAndUnclassifiedIsAnExplicitNull() {
        val cleared = JsonParser.parseString(LabelChange(pillar = PillarChoice.Unclassified).toJson()).asJsonObject
        assertEquals(setOf("kakeibo_type"), cleared.keySet())
        assertTrue("Belum diklasifikasi is null, never a missing key", cleared["kakeibo_type"].isJsonNull)

        val full = JsonParser.parseString(LabelChange(category, PillarChoice.Pillar("want"), "  Kopi Contoh ").toJson()).asJsonObject
        assertEquals(category, full["category_id"].asString)
        assertEquals("want", full["kakeibo_type"].asString)
        assertEquals("Kopi Contoh", full["merchant"].asString)

        assertEquals(setOf("category_id"), JsonParser.parseString(LabelChange(categoryId = category).toJson()).asJsonObject.keySet())
        assertFalse(JsonParser.parseString(LabelChange(categoryId = category, merchant = " ").toJson()).asJsonObject.has("merchant"))
    }

    @Test fun aChangeThatCannotBeSentSaysWhy() {
        assertEquals("Pilih kategori, pilar, atau isi merchant dulu.", LabelChange().problem("expense"))
        assertEquals("Pilar Kakeibo hanya untuk pengeluaran.", LabelChange(pillar = PillarChoice.Unclassified).problem("income"))
        assertEquals("Label transaksi ini hanya bisa diubah di web.", LabelChange(categoryId = category).problem("internal_movement"))
        assertEquals("Pilar tidak dikenal.", LabelChange(pillar = PillarChoice.Pillar("luxury")).problem("expense"))
        assertEquals("Kategori tidak dikenal. Muat ulang daftar kategori.", LabelChange(categoryId = "makan").problem("expense"))
        assertNull(LabelChange(categoryId = category).problem("income"))
        assertNull(LabelChange(pillar = PillarChoice.Unclassified).problem("expense"))
    }

    /** The view model works on the main looper, so the test lets it run instead of blocking that thread. */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun correct(rowId: Long, change: LabelChange): Pair<Boolean, String?> {
        val done = CompletableDeferred<Pair<Boolean, String?>>()
        model.correctLabel(rowId, change) { ok, message -> done.complete(ok to message) }
        val deadline = System.currentTimeMillis() + 10_000
        while (!done.isCompleted && System.currentTimeMillis() < deadline) {
            shadowOf(Looper.getMainLooper()).idle()
            Thread.sleep(10)
        }
        return done.getCompleted()
    }

    @Test fun aRecordedExpenseIsPatchedThroughTheOwnerEditPath() = runBlocking {
        val pairing = AppPreferences(app).pairingContext
        db.notificationDao().insertOrIgnore(UiFixtures.recorded(20, "Kopi Contoh", 32_000, hour = 10).copy(pairingContext = pairing))
        val (ok, message) = correct(20, LabelChange(categoryId = category, pillar = PillarChoice.Unclassified))
        assertTrue(message, ok)
        val request = patches.single()
        assertEquals("/api/transactions/${UiFixtures.uuid(530)}", request.path)
        val body = JsonParser.parseString(request.body.readUtf8()).asJsonObject
        assertEquals(setOf("category_id", "kakeibo_type"), body.keySet())
        assertTrue(body["kakeibo_type"].isJsonNull)
        assertFalse("amount, account, date and type stay as recorded", body.has("amount") || body.has("account_id") || body.has("date") || body.has("type"))
    }

    @Test fun aRefusedCorrectionChangesNothingAndSaysWhy() = runBlocking {
        val pairing = AppPreferences(app).pairingContext
        db.notificationDao().insertOrIgnore(UiFixtures.recorded(20, "Kopi Contoh", 32_000).copy(pairingContext = pairing))
        patchReply = MockResponse().setResponseCode(409).setBody("""{"detail":"linked"}""")
        assertEquals(false to "Transaksi ini terhubung dengan transaksi lain. Ubah lewat web.", correct(20, LabelChange(categoryId = category)))
    }

    @Test fun rowsThatAreNotRecordedHereAreNeverPatched() = runBlocking {
        db.notificationDao().insertOrIgnore(UiFixtures.recorded(20, "Kopi Contoh", 32_000)) // another pairing's row
        db.notificationDao().insertOrIgnore(UiFixtures.review().copy(pairingContext = AppPreferences(app).pairingContext))
        assertEquals(false to "Label transaksi ini hanya bisa diubah di web.", correct(20, LabelChange(categoryId = category)))
        assertEquals(false to "Label transaksi ini hanya bisa diubah di web.", correct(UiFixtures.review().id, LabelChange(categoryId = category)))
        assertTrue(patches.isEmpty())
    }
}
