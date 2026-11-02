package com.alfonsusenrico.financialtracker.listener.ui

import android.app.Application
import androidx.compose.ui.test.junit4.ComposeContentTestRule
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.room.Room
import androidx.sqlite.db.SupportSQLiteOpenHelper
import androidx.test.espresso.IdlingRegistry
import androidx.test.espresso.IdlingResource
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import kotlinx.coroutines.cancel
import kotlinx.coroutines.job
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Runs Room's queries, flow updates and transactions on [threads] and stays busy for Espresso's idling registry
 * while any of them is queued or running. Compose's waits and assertions under Robolectric go through
 * `Espresso.onIdle()`, which then also drains what the finished task posted to the main thread, so a screen is
 * checked only after Room has delivered the rows it shows.
 */
class RoomIdlingExecutor(private val threads: ExecutorService = Executors.newCachedThreadPool()) : Executor, IdlingResource {
    private val pending = AtomicInteger()

    @Volatile private var onIdle: IdlingResource.ResourceCallback? = null

    override fun execute(task: Runnable) {
        pending.incrementAndGet()
        try {
            threads.execute {
                try {
                    task.run()
                } finally {
                    finished()
                }
            }
        } catch (rejected: RejectedExecutionException) {
            finished()
            throw rejected
        }
    }

    private fun finished() {
        if (pending.decrementAndGet() == 0) onIdle?.onTransitionToIdle()
    }

    override fun getName(): String = "Room executor"

    override fun isIdleNow(): Boolean = pending.get() == 0

    override fun registerIdleTransitionCallback(callback: IdlingResource.ResourceCallback) {
        onIdle = callback
    }

    /** Stops taking work and waits for the threads, so nothing Room started can fail after the test that started it. */
    fun shutdown() {
        threads.shutdown()
        check(threads.awaitTermination(5, TimeUnit.SECONDS)) { "Room work was still running 5 s after the test" }
    }
}

/**
 * An in-memory [AppDatabase] whose Room work [compose] waits for, and the view models that use it.
 * [openHelperFactory] lets a test slow SQLite down to prove that waiting.
 */
class ShellDatabase(
    app: Application,
    private val compose: ComposeContentTestRule,
    openHelperFactory: SupportSQLiteOpenHelper.Factory? = null
) {
    private val room = RoomIdlingExecutor()
    private val viewModels = mutableListOf<ViewModel>()

    val db: AppDatabase = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java)
        .setQueryExecutor(room)
        .setTransactionExecutor(room)
        .apply { if (openHelperFactory != null) openHelperFactory(openHelperFactory) }
        .build()

    init {
        IdlingRegistry.getInstance().register(room)
    }

    /** A view model over [db]; [close] stops it before the database closes. */
    fun <T : ViewModel> track(viewModel: T): T = viewModel.also { viewModels += it }

    /**
     * Cancels the view models and waits until their coroutines have finished and Room is idle, then closes the
     * database. A cancelled coroutine still finishes the query it is running, and the screen keeps reading rows until
     * the rule disposes it after this. Closing under such a query fails it in a coroutine no test owns, and
     * kotlinx-coroutines-test then fails the next test with UncaughtExceptionsBeforeTest.
     */
    fun close() {
        try {
            viewModels.forEach { it.viewModelScope.cancel() }
            compose.waitUntil(STOP_TIMEOUT_MILLIS) {
                // waitUntil itself does not run the main looper, where the cancelled coroutines finish.
                compose.waitForIdle()
                viewModels.all { it.viewModelScope.coroutineContext.job.isCompleted }
            }
            db.close()
        } finally {
            IdlingRegistry.getInstance().unregister(room)
            room.shutdown()
        }
    }

    private companion object {
        const val STOP_TIMEOUT_MILLIS = 5_000L
    }
}
