package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.util.Log
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.sync.AutoSync
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

class FinancialTrackerApp : Application() {

    companion object {
        const val TAG = "FinancialTrackerApp"
    }

    override fun onCreate() {
        super.onCreate()
        Log.i(TAG, "Initializing Financial Tracker Capture Companion...")

        // Initialize Room DB instance eagerly
        AppDatabase.getDatabase(this)
        CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
            SenderReplies(this@FinancialTrackerApp).recover()
            TextCaptures(this@FinancialTrackerApp).recover()
        }

        AutoSync.restore(this)
    }
}
