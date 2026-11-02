package com.alfonsusenrico.financialtracker.listener.data.network

import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountsEnvelope
import com.alfonsusenrico.financialtracker.listener.data.network.model.BatchIngestRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.BatchIngestResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureAnswerRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureEnvelope
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSuggestionsResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureTextRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoriesEnvelope
import com.alfonsusenrico.financialtracker.listener.data.network.model.HealthResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.LatestReportResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.MappingConfirmRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResolveRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResultResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeAnswerRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.SourcePocketOptionsResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.TransactionUpdateRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.TransactionUpdateResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.SenderConfirmRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.SourcePocketConfirmRequest
import okhttp3.RequestBody
import okhttp3.ResponseBody
import retrofit2.Response
import retrofit2.http.Body
import retrofit2.http.DELETE
import retrofit2.http.GET
import retrofit2.http.PATCH
import retrofit2.http.POST
import retrofit2.http.Path
import retrofit2.http.Query

interface IngestionApiService {

    /** The device health contract of 2026-11-02; the body is built with explicit nulls ([HealthSnapshot.toJson]). */
    @POST("api/devices/health")
    suspend fun reportHealth(@Body body: RequestBody): Response<ResponseBody>

    @GET("api/reports/monthly/latest")
    suspend fun latestMonthlyReport(): Response<LatestReportResponse>

    @GET("api/ingest/notifications/{eventId}/source-pockets")
    suspend fun getSourcePockets(@Path("eventId") eventId: String): Response<SourcePocketOptionsResponse>

    @POST("api/ingest/notifications/{eventId}/confirm-source-pocket")
    suspend fun confirmSourcePocket(
        @Path("eventId") eventId: String,
        @Body request: SourcePocketConfirmRequest
    ): Response<NotificationResultResponse>

    @POST("api/ingest/notifications/{eventId}/confirm-sender")
    suspend fun confirmSender(
        @Path("eventId") eventId: String,
        @Body request: SenderConfirmRequest
    ): Response<NotificationResultResponse>

    @POST("api/ingest/notifications/{eventId}/answer-purpose")
    suspend fun answerPurpose(
        @Path("eventId") eventId: String,
        @Body request: PurposeAnswerRequest
    ): Response<NotificationResultResponse>

    @GET("api/health")
    suspend fun checkHealth(): Response<HealthResponse>

    @POST("api/ingest/notifications")
    suspend fun ingestNotifications(
        @Body request: BatchIngestRequest
    ): Response<BatchIngestResponse>

    @GET("api/ingest/notifications/{eventId}/result")
    suspend fun getNotificationResult(@Path("eventId") eventId: String): Response<NotificationResultResponse>

    @POST("api/ingest/notifications/{eventId}/retry")
    suspend fun retryNotification(@Path("eventId") eventId: String): Response<NotificationResultResponse>

    @POST("api/ingest/notifications/{eventId}/confirm-mapping")
    suspend fun confirmMapping(
        @Path("eventId") eventId: String,
        @Body request: MappingConfirmRequest
    ): Response<NotificationResultResponse>

    @POST("api/ingest/notifications/{eventId}/resolve")
    suspend fun resolveNotification(
        @Path("eventId") eventId: String,
        @Body request: NotificationResolveRequest
    ): Response<NotificationResultResponse>

    @GET("api/accounts")
    suspend fun getAccounts(): Response<AccountsEnvelope>

    @GET("api/categories")
    suspend fun getCategories(): Response<CategoriesEnvelope>

    @PATCH("api/transactions/{id}")
    suspend fun updateTransaction(
        @Path("id") id: String,
        @Body request: TransactionUpdateRequest
    ): Response<TransactionUpdateResponse>

    /** Perbaiki label: only the fields the owner changed, with an explicit null to clear the pillar. */
    @PATCH("api/transactions/{id}")
    suspend fun correctLabel(@Path("id") id: String, @Body body: RequestBody): Response<TransactionUpdateResponse>

    @DELETE("api/transactions/{id}")
    suspend fun deleteTransaction(@Path("id") id: String): Response<TransactionUpdateResponse>

    @POST("api/capture/text")
    suspend fun captureText(@Body request: CaptureTextRequest): Response<CaptureEnvelope>

    @POST("api/capture/{id}/answer")
    suspend fun answerCapture(@Path("id") id: String, @Body request: CaptureAnswerRequest): Response<CaptureEnvelope>

    @POST("api/capture/{id}/undo")
    suspend fun undoCapture(@Path("id") id: String): Response<CaptureEnvelope>

    @GET("api/capture/{id}")
    suspend fun getCapture(@Path("id") id: String): Response<CaptureEnvelope>

    @GET("api/capture/suggestions")
    suspend fun captureSuggestions(@Query("device_time") deviceTime: String): Response<CaptureSuggestionsResponse>
}
