package com.alfonsusenrico.financialtracker.listener.ui

import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureMovement
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureOption
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSummary
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSuggestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureTransaction
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureView
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.google.gson.Gson
import java.time.LocalDate
import java.time.ZoneId

/** Synthetic typed captures in every state; names, texts and amounts are invented. */
object CaptureFixtures {
    const val NOW = 1_800_000_000_000L
    private val gson = Gson()
    private fun uuid(n: Int) = "00000000-0000-4000-8000-%012d".format(n)
    val today: LocalDate = UiFixtures.today

    private fun time(hour: Int, date: LocalDate = today) = date.atTime(hour, 15).atZone(ZoneId.systemDefault())

    fun entity(
        id: Long, text: String, state: String, view: CaptureView? = null, question: CaptureQuestion? = null,
        error: String? = null, attempts: Int = 0, undoDeadline: Long? = null, pairing: String? = UiFixtures.PAIRING,
        hour: Int = 9, edited: Boolean = false
    ) = TextCaptureEntity(
        id = id, clientCaptureId = uuid(1000 + id.toInt()), text = text, deviceTime = time(hour).toOffsetDateTime().toString(),
        createdAt = time(hour).toInstant().toEpochMilli(), state = state,
        serverId = if (view != null) uuid(2000 + id.toInt()) else null,
        pairingContext = if (view != null) pairing else null,
        questionJson = question?.let(gson::toJson), summaryJson = view?.let(gson::toJson),
        transactionId = view?.transaction?.id, undoDeadline = undoDeadline, attempts = attempts, errorCode = error, edited = edited
    )

    private fun transaction(id: Long, type: String, amount: Long, merchant: String, account: String, category: String?, kakeibo: String?, hour: Int, date: LocalDate) =
        CaptureTransaction(
            id = uuid(3000 + id.toInt()), type = type, amount = amount, date = time(hour, date).toOffsetDateTime().toString(),
            accountName = account, merchant = merchant, notes = merchant, categoryName = category, kakeibo = kakeibo, source = "capture"
        )

    fun recorded(
        id: Long = 1, text: String = "bayar parkir contoh 10rb", amount: Long = 10_000, merchant: String = "Parkir Contoh",
        type: String = "expense", category: String? = "Parkir", undoDeadline: Long? = NOW + 10_000, hour: Int = 9,
        date: LocalDate = today, edited: Boolean = false
    ) = entity(
        id, text, TextCaptureState.RECORDED,
        CaptureView(
            id = uuid(2000 + id.toInt()), status = "recorded", text = text,
            summary = CaptureSummary(type, amount, date.toString(), merchant),
            transaction = transaction(id, type, amount, merchant, "Tunai", category, if (type == "expense") "need" else null, hour, date)
        ),
        undoDeadline = undoDeadline, hour = hour, edited = edited
    )

    fun movement(id: Long = 2, hour: Int = 8) = entity(
        id, "tarik tunai 200rb", TextCaptureState.RECORDED,
        CaptureView(
            id = uuid(2000 + id.toInt()), status = "recorded", text = "tarik tunai 200rb",
            summary = CaptureSummary("movement", 200_000, today.toString(), "Tarik tunai"),
            transaction = transaction(id, "expense", 200_000, "Tarik tunai", "Bank Contoh", null, null, hour, today),
            movement = CaptureMovement(
                uuid(4000), CaptureTransaction(id = uuid(3000 + id.toInt()), accountName = "Bank Contoh"),
                CaptureTransaction(id = uuid(5000), accountName = "Tunai")
            )
        ),
        undoDeadline = 0, hour = hour
    )

    fun accountQuestion(withOther: Boolean = true) = CaptureQuestion(
        questionId = uuid(6001), type = "account", role = "source", prompt = "Dari rekening mana?",
        options = listOf(CaptureOption(uuid(7001), "Tunai", "cash"), CaptureOption(uuid(7002), "Dompet Contoh", "ewallet")),
        allowsText = false, allowsOther = withOther
    )

    fun amountQuestion() = CaptureQuestion(
        questionId = uuid(6002), type = "amount", reason = "missing", prompt = "Berapa nominalnya?", options = emptyList(), allowsText = true
    )

    fun categoryQuestion() = CaptureQuestion(
        questionId = uuid(6003), type = "category", prompt = "Kategorinya apa?",
        options = listOf(CaptureOption("c1", "Makan & Minum", categoryId = uuid(8001), kakeibo = "need"), CaptureOption("c2", "Jajan", categoryId = uuid(8002), kakeibo = "want")),
        allowsOther = true
    )

    fun asking(id: Long = 3, question: CaptureQuestion = accountQuestion(), amount: Long? = 45_000) = entity(
        id, "makan siang 45rb", TextCaptureState.NEEDS_DETAIL,
        CaptureView(id = uuid(2000 + id.toInt()), status = "needs_detail", summary = CaptureSummary("expense", amount, today.toString(), "makan siang")),
        question = question, hour = 12
    )

    fun queued(id: Long = 4, error: String? = TextCaptures.NETWORK, attempts: Int = 1) =
        entity(id, "kopi contoh 32rb", TextCaptureState.QUEUED, error = error, attempts = attempts, hour = 10)

    fun sending(id: Long = 5) = entity(id, "ojek contoh 24rb", TextCaptureState.SENDING, attempts = 1, hour = 11)

    fun failed(id: Long = 6) = entity(id, "besok makan 20rb", TextCaptureState.FAILED, error = "invalid_capture_date", attempts = 1, hour = 7)

    fun undone(id: Long = 7) = entity(
        id, "kopi contoh 32rb", TextCaptureState.UNDONE,
        CaptureView(id = uuid(2000 + id.toInt()), status = "undone", summary = CaptureSummary("expense", 32_000, today.toString(), "kopi contoh"))
    )

    val suggestions = listOf(
        CaptureSuggestion("parkir contoh 10rb", "Parkir Contoh", 10_000, "Parkir", "Tunai", "biasanya jam segini"),
        CaptureSuggestion("kopi contoh 32rb", "Kopi Contoh", 32_000, "Makan & Minum", "Dompet Contoh", "sering")
    )
}
