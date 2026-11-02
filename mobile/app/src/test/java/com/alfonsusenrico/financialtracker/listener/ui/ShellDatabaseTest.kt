package com.alfonsusenrico.financialtracker.listener.ui

import android.app.Application
import android.database.Cursor
import android.os.CancellationSignal
import android.os.Looper
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithText
import androidx.sqlite.db.SupportSQLiteDatabase
import androidx.sqlite.db.SupportSQLiteOpenHelper
import androidx.sqlite.db.SupportSQLiteQuery
import androidx.sqlite.db.framework.FrameworkSQLiteOpenHelperFactory
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionApp
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.PhoneState
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/**
 * The shell harness over a slow SQLite: a sheet is checked only after Room delivered its row. Without the Room
 * idling resource this fails on every run, as the shell tests did now and then on a busy CI runner.
 */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class, qualifiers = "w411dp-h1600dp")
class ShellDatabaseTest {
    @get:Rule val compose = createComposeRule()

    private val app = ApplicationProvider.getApplicationContext<Application>()
    private lateinit var database: ShellDatabase

    @Before fun open() {
        database = ShellDatabase(app, compose, SlowSqlite(SLOW_QUERY_MILLIS))
        val pairing = AppPreferences(app).pairingContext
        runBlocking {
            listOf(UiFixtures.purpose(), UiFixtures.sender(), UiFixtures.review())
                .forEach { database.db.notificationDao().insertOrIgnore(it.copy(pairingContext = pairing)) }
        }
    }

    @After fun close() = database.close()

    private fun shell(): CompanionViewModel {
        val viewModel = database.track(CompanionViewModel(app, database.db))
        val captures = database.track(CaptureViewModel(app, database.db))
        compose.setContent {
            CompanionTheme(dark = false) {
                CompanionApp(viewModel, captures, PhoneState(), now = { UiFixtures.at(UiFixtures.today, 10, 0) }, listener = MutableStateFlow(true))
            }
        }
        return viewModel
    }

    private val heading = SemanticsMatcher.keyIsDefined(SemanticsProperties.Heading)

    @Test fun aSheetIsCheckedOnlyAfterItsRowArrives() {
        shell().requestOpen(UiFixtures.sender().id)
        compose.onNode(hasText("Pertanyaan") and heading).assertExists()
        compose.onNodeWithText("1 dari 2 menunggu").assertExists()
        compose.onNodeWithText("Perlu sekarang").assertExists()
    }

    private companion object {
        const val SLOW_QUERY_MILLIS = 40L
    }
}

/** Holds every background query between preparing it and reading its rows, where a closing database fails it. */
private class SlowSqlite(private val delayMillis: Long) : SupportSQLiteOpenHelper.Factory {
    private val framework = FrameworkSQLiteOpenHelperFactory()

    override fun create(configuration: SupportSQLiteOpenHelper.Configuration): SupportSQLiteOpenHelper =
        SlowHelper(framework.create(configuration), delayMillis)
}

private class SlowHelper(private val helper: SupportSQLiteOpenHelper, private val delayMillis: Long) :
    SupportSQLiteOpenHelper by helper {
    override val writableDatabase: SupportSQLiteDatabase get() = SlowDatabase(helper.writableDatabase, delayMillis)
    override val readableDatabase: SupportSQLiteDatabase get() = SlowDatabase(helper.readableDatabase, delayMillis)
}

private class SlowDatabase(private val database: SupportSQLiteDatabase, private val delayMillis: Long) :
    SupportSQLiteDatabase by database {
    private fun slow(cursor: Cursor): Cursor {
        if (Looper.myLooper() != Looper.getMainLooper()) Thread.sleep(delayMillis)
        return cursor
    }

    override fun query(query: String): Cursor = slow(database.query(query))

    override fun query(query: SupportSQLiteQuery): Cursor = slow(database.query(query))

    override fun query(query: SupportSQLiteQuery, cancellationSignal: CancellationSignal?): Cursor =
        slow(database.query(query, cancellationSignal))
}
