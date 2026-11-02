package com.alfonsusenrico.financialtracker.listener.ui.capture

import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureActionResult
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures

/** What the owner reads after each capture action: what happened, and what is unchanged. */
object CaptureMessages {
    const val OFFLINE = "Koneksi tidak tersedia."
    const val OTHER_PAIRING = "Catatan ini dari akun tracker lain. Hubungkan kembali akun itu."
    const val AUTH = "Kunci tracker ditolak. Hubungkan kembali di Atur."

    fun answer(result: CaptureActionResult): String? = when (result) {
        CaptureActionResult.DONE -> null
        CaptureActionResult.OFFLINE -> "$OFFLINE Jawaban tersimpan dan bisa dikirim ulang."
        CaptureActionResult.SERVER_ERROR -> "Tracker belum menjawab. Jawaban tersimpan dan bisa dikirim ulang."
        CaptureActionResult.AUTH -> "$AUTH Jawaban tersimpan."
        CaptureActionResult.INVALID -> "Jawaban belum diterima. Periksa lalu jawab lagi."
        CaptureActionResult.STALE, CaptureActionResult.CONFLICT -> "Pertanyaan sudah berubah. Lihat pertanyaan terbaru."
        CaptureActionResult.OTHER_PAIRING -> OTHER_PAIRING
        else -> "Catatan ini tidak lagi menunggu jawaban."
    }

    fun undo(result: CaptureActionResult): String? = when (result) {
        CaptureActionResult.DONE -> null
        CaptureActionResult.TOO_LATE -> "Waktu membatalkan sudah habis. Transaksi tetap tersimpan; hapus lewat Rincian bila perlu."
        CaptureActionResult.CHANGED -> "Transaksi sudah diubah, jadi tidak bisa dibatalkan. Hapus lewat Rincian bila perlu."
        CaptureActionResult.OFFLINE -> "$OFFLINE Belum dibatalkan."
        CaptureActionResult.AUTH -> "$AUTH Belum dibatalkan."
        CaptureActionResult.OTHER_PAIRING -> OTHER_PAIRING
        CaptureActionResult.NOT_FOUND -> "Catatan tidak ditemukan di tracker."
        else -> "Tracker belum bisa membatalkan. Belum dibatalkan."
    }

    fun edit(result: CaptureActionResult): String = when (result) {
        CaptureActionResult.DONE -> "Transaksi diperbarui di tracker."
        CaptureActionResult.OFFLINE -> "$OFFLINE Tidak ada yang diubah."
        CaptureActionResult.AUTH -> "$AUTH Tidak ada yang diubah."
        CaptureActionResult.NOT_FOUND -> "Transaksi tidak ditemukan di tracker."
        CaptureActionResult.NOT_ALLOWED -> "Transaksi ini tidak bisa diubah di aplikasi."
        else -> "Tracker menolak perubahan. Tidak ada yang diubah."
    }

    fun delete(result: CaptureActionResult): String = when (result) {
        CaptureActionResult.DONE -> "Dihapus dari tracker."
        CaptureActionResult.NOT_FOUND -> "Transaksi ini sudah tidak ada di tracker. Mungkin sudah dihapus di web."
        CaptureActionResult.OFFLINE -> "$OFFLINE Tidak ada yang dihapus."
        CaptureActionResult.AUTH -> "$AUTH Tidak ada yang dihapus."
        CaptureActionResult.CONFLICT -> "Tracker menolak: transaksi ini terhubung dengan transaksi lain. Hapus lewat Tracker Web."
        CaptureActionResult.NOT_ALLOWED -> "Hanya transaksi dari catatan ini yang bisa dihapus dari aplikasi."
        else -> "Tracker belum bisa menghapus. Tidak ada yang dihapus."
    }

    /** Why a queued or refused capture is waiting; null when it simply has not been sent yet. */
    fun waiting(errorCode: String?): String? = when (errorCode) {
        TextCaptures.NETWORK -> "Ponsel belum tersambung ke tracker."
        TextCaptures.SERVER -> "Tracker tidak menjawab. Teks kamu masih tersimpan."
        TextCaptures.AUTH -> "$AUTH Teks kamu masih tersimpan."
        TextCaptures.NOT_PAIRED -> "Belum ada tracker yang terhubung. Hubungkan di Atur."
        null -> null
        else -> refused(errorCode)
    }

    /** The tracker's reason for refusing a capture. */
    fun refused(errorCode: String?): String = when (errorCode) {
        "invalid_capture_text" -> "Teks harus 1–200 karakter tanpa karakter khusus."
        "invalid_device_time" -> "Jam ponsel tidak cocok dengan tracker. Periksa pengaturan waktu, lalu coba lagi."
        "invalid_capture_date" -> "Tanggal di teks tidak valid atau masih di masa depan."
        "capture_id_conflict" -> "Tracker sudah menyimpan catatan lain dengan kode yang sama."
        else -> "Tracker menolak catatan ini${errorCode?.let { " ($it)" }.orEmpty()}."
    }
}
