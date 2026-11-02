package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.test.runner.AndroidJUnitRunner

class VerificationTestRunner : AndroidJUnitRunner() {
    override fun newApplication(cl: ClassLoader, className: String, context: Context): Application {
        check(context.packageName.endsWith(".verification")) { "Device tests require the verification app" }
        return super.newApplication(cl, TestApplication::class.java.name, context)
    }
}
