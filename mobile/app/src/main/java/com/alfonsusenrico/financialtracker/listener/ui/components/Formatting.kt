package com.alfonsusenrico.financialtracker.listener.ui.components

import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

/** Indonesian notation used across the companion: "Rp187.500", "10.45", "Selasa, 27 Okt". */
object Formatting {
    private val indonesian = Locale("id", "ID")
    private val timeFormat = DateTimeFormatter.ofPattern("HH.mm", indonesian)
    private val dayMonthFormat = DateTimeFormatter.ofPattern("EEEE, d MMM", indonesian)
    private val dateTimeFormat = DateTimeFormatter.ofPattern("d MMM yyyy, HH.mm", indonesian)

    fun rupiah(amount: Long): String = PurposeQuestions.rupiah(amount)

    fun localDate(millis: Long, zone: ZoneId = ZoneId.systemDefault()): LocalDate =
        Instant.ofEpochMilli(millis).atZone(zone).toLocalDate()

    fun time(millis: Long, zone: ZoneId = ZoneId.systemDefault()): String =
        timeFormat.format(Instant.ofEpochMilli(millis).atZone(zone))

    fun dateTime(millis: Long, zone: ZoneId = ZoneId.systemDefault()): String =
        dateTimeFormat.format(Instant.ofEpochMilli(millis).atZone(zone))

    /** A server timestamp such as "2026-10-30T03:00:00+00:00", shown in the server's reporting zone. */
    fun dateTime(isoOffset: String, zone: ZoneId): String =
        dateTimeFormat.format(OffsetDateTime.parse(isoOffset).atZoneSameInstant(zone))

    fun dayMonth(date: LocalDate): String = dayMonthFormat.format(date)

    /** "Hari ini", "Kemarin", or the weekday name for older days. */
    fun relativeDay(date: LocalDate, today: LocalDate): String = when (date) {
        today -> "Hari ini"
        today.minusDays(1) -> "Kemarin"
        else -> date.format(DateTimeFormatter.ofPattern("EEEE", indonesian))
    }
}
