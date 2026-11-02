package com.alfonsusenrico.financialtracker.listener.data.network

import com.google.gson.annotations.SerializedName
import retrofit2.Response
import retrofit2.http.Body
import retrofit2.http.DELETE
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path

class PairRequest(
    val code: String,
    @SerializedName("device_id") val deviceId: String,
    @SerializedName("device_name") val deviceName: String
)
class PairResponse(val ok: Boolean, @SerializedName("api_key") val apiKey: String?)
class Identity(val id: String?)
class IdentityResponse(val ok: Boolean, val user: Identity?)
class DeviceItem(val id: String?, @SerializedName("key_prefix") val prefix: String?)
class DevicesResponse(val ok: Boolean, val devices: List<DeviceItem>?)

interface PairingApiService {
    @POST("api/devices/pair") suspend fun exchange(@Body request: PairRequest): Response<PairResponse>
    @GET("api/auth/me") suspend fun identity(): Response<IdentityResponse>
    @GET("api/devices") suspend fun devices(): Response<DevicesResponse>
    @DELETE("api/devices/{id}") suspend fun revoke(@Path("id") id: String): Response<okhttp3.ResponseBody>
}
