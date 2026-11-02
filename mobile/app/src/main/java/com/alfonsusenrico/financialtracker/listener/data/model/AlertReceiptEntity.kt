package com.alfonsusenrico.financialtracker.listener.data.model

import androidx.room.ColumnInfo
import androidx.room.Entity

@Entity(tableName = "alert_receipts", primaryKeys = ["pairing_context", "record_key"])
data class AlertReceiptEntity(
    @ColumnInfo(name = "pairing_context") val pairingContext: String,
    @ColumnInfo(name = "record_key") val recordKey: String,
    @ColumnInfo(name = "delivered_at") val deliveredAt: Long? = null
)
