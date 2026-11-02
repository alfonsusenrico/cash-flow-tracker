package com.alfonsusenrico.financialtracker.listener.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.ComponentName
import android.content.Context
import android.graphics.BitmapFactory
import android.os.Build
import android.provider.Settings
import com.alfonsusenrico.financialtracker.listener.R
import android.service.notification.NotificationListenerService
import android.service.notification.StatusBarNotification
import android.util.Log
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.AutoSync
import com.alfonsusenrico.financialtracker.listener.registry.FinancialSourceRegistry
import androidx.core.app.NotificationCompat
import com.google.gson.Gson
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

class FinancialNotificationListenerService : NotificationListenerService() {

    companion object {
        const val TAG = "FinancialNotifListener"
        private const val FOREGROUND_NOTIFICATION_ID = 1001
        private const val CHANNEL_ID = "listener_active"

        private val _isConnected = MutableStateFlow(false)
        val isConnected: StateFlow<Boolean> = _isConnected.asStateFlow()

        fun isPermissionGranted(context: Context): Boolean {
            val enabledListeners = Settings.Secure.getString(
                context.contentResolver,
                "enabled_notification_listeners"
            ) ?: return false

            val myComponent = ComponentName(context, FinancialNotificationListenerService::class.java).flattenToString()
            return enabledListeners.contains(myComponent)
        }
    }

    private lateinit var processor: NotificationProcessor
    private lateinit var prefs: AppPreferences
    private val gson = Gson()

    override fun onCreate() {
        super.onCreate()
        prefs = AppPreferences(applicationContext)
        processor = NotificationProcessor(applicationContext)
        ensureNotificationChannel()
        Log.i(TAG, "FinancialNotificationListenerService created")
    }

    override fun onListenerConnected() {
        super.onListenerConnected()
        _isConnected.value = true
        prefs.recordListenerConnected()
        Log.i(TAG, "Notification Listener connected. Scanning active notifications and starting foreground...")

        startListenerForeground()

        try {
            val currentActive = activeNotifications
            Log.i(TAG, "Found ${currentActive?.size ?: 0} active notifications in drawer")
            currentActive?.forEach { sbn ->
                handleStatusBarNotification(sbn)
            }
        } catch (e: Exception) {
            Log.e(TAG, "Could not process active notifications")
        }

        // Flush any notifications that arrived while the service was suspended.
        AutoSync.sendPending(applicationContext)
    }

    override fun onListenerDisconnected() {
        super.onListenerDisconnected()
        _isConnected.value = false
        prefs.recordListenerDisconnected()
        stopForeground(STOP_FOREGROUND_REMOVE)
        Log.w(TAG, "Notification Listener disconnected; requesting rebind")
        requestRebind(ComponentName(this, FinancialNotificationListenerService::class.java))
    }

    override fun onNotificationPosted(sbn: StatusBarNotification?) {
        super.onNotificationPosted(sbn)
        if (sbn == null) return
        handleStatusBarNotification(sbn)
    }

    private fun handleStatusBarNotification(sbn: StatusBarNotification) {
        val packageName = sbn.packageName ?: return

        // Privacy Gate: Strictly discard non-financial notifications
        if (!FinancialSourceRegistry.isTargetFinancialApp(packageName)) {
            return
        }

        // User Setting Gate: Only process apps enabled in Settings
        if (!prefs.isPackageEnabled(packageName)) {
            Log.d(TAG, "Ignoring notification: $packageName is disabled in User Settings")
            return
        }

        try {
            val notification = sbn.notification ?: return
            val extras = notification.extras
            val isGroupSummary = notification.flags and Notification.FLAG_GROUP_SUMMARY != 0

            val title = extras?.getCharSequence(Notification.EXTRA_TITLE)?.toString()?.trim()
            val text = extras?.getCharSequence(Notification.EXTRA_TEXT)?.toString()?.trim()
            val bigText = extras?.getCharSequence(Notification.EXTRA_BIG_TEXT)?.toString()?.trim()
            val subText = extras?.getCharSequence(Notification.EXTRA_SUB_TEXT)?.toString()?.trim()
            val summaryText = extras?.getCharSequence(Notification.EXTRA_SUMMARY_TEXT)?.toString()?.trim()
            // A group summary's inbox lines repeat its children, which are captured on their own.
            val textLines = if (isGroupSummary) emptyList() else
                extras?.getCharSequenceArray(Notification.EXTRA_TEXT_LINES).orEmpty().map { it.toString() }
            val messages = NotificationCompat.MessagingStyle.extractMessagingStyleFromNotification(notification)
                ?.messages.orEmpty().mapNotNull { it.text?.toString() }

            // Extract string/primitive extras safely into a lightweight JSON map for dataset analysis
            val extrasMap = mutableMapOf<String, String>()
            if (extras != null) {
                for (key in extras.keySet()) {
                    @Suppress("DEPRECATION")
                    val value = extras.get(key)
                    if (value is String || value is CharSequence || value is Number || value is Boolean) {
                        extrasMap[key] = value.toString()
                    }
                }
            }
            val rawExtrasJson = if (extrasMap.isNotEmpty()) gson.toJson(extrasMap) else null

            processor.enqueue(
                CapturedNotification(
                    packageName = packageName,
                    notificationKey = sbn.key,
                    notificationId = sbn.id,
                    notificationWhen = notification.`when`,
                    channelId = notification.channelId,
                    category = notification.category,
                    title = title,
                    bodyText = text,
                    bigText = NotificationProcessor.expandedBigText(title, text, bigText, textLines + messages),
                    subText = subText,
                    summaryText = summaryText,
                    postTime = sbn.postTime,
                    rawExtrasJson = rawExtrasJson,
                    isOngoing = notification.flags and
                        (Notification.FLAG_ONGOING_EVENT or Notification.FLAG_FOREGROUND_SERVICE) != 0,
                    isGroupSummary = isGroupSummary
                )
            )
        } catch (e: Exception) {
            Log.e(TAG, "Could not handle notification")
        }
    }

    private fun ensureNotificationChannel() {
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "Listener Active",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Keeps the financial notification listener running in the background"
                setShowBadge(false)
            }
            manager.createNotificationChannel(channel)
        }
    }

    private fun startListenerForeground() {
        try {
            val appIconBitmap = BitmapFactory.decodeResource(resources, R.mipmap.ic_launcher)
            val notification = Notification.Builder(this, CHANNEL_ID)
                .setContentTitle("Financial Tracker")
                .setContentText("Monitoring transactions...")
                .setSmallIcon(R.drawable.ic_notification)
                .setLargeIcon(appIconBitmap)
                .setOngoing(true)
                .build()

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                startForeground(
                    FOREGROUND_NOTIFICATION_ID,
                    notification,
                    android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE
                )
            } else {
                startForeground(FOREGROUND_NOTIFICATION_ID, notification)
            }
            Log.i(TAG, "Foreground service notification started")
        } catch (e: SecurityException) {
            Log.w(TAG, "Could not start foreground service: permission denied")
        } catch (e: Exception) {
            Log.w(TAG, "Foreground service start failed")
        }
    }
}
