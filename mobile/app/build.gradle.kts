import java.net.URI

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.compose")
    id("com.google.devtools.ksp")
}

ksp {
    arg("room.schemaLocation", "$projectDir/schemas")
}

// The source version; debug builds and untagged release builds keep it unchanged.
val sourceVersionName = "2.4.0"
val sourceVersionCode = 27

// A tagged release (android-vX.Y.Z) passes X.Y.Z. The code comes from the numbers, so every newer
// version installs over the previous one; MINOR and PATCH stay below 100 to keep that ordering.
fun releaseVersionCode(version: String): Int {
    val match = Regex("""^(0|[1-9][0-9]*)\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)$""").matchEntire(version)
        ?: error("companionReleaseVersion must be MAJOR.MINOR.PATCH with MINOR and PATCH from 0 to 99, got '$version'")
    val (major, minor, patch) = match.destructured.toList().map(String::toInt)
    return major * 10000 + minor * 100 + patch
}

val releaseVersionName: String? = providers.gradleProperty("companionReleaseVersion").orNull?.trim()
val publishedVersionCode: Int? = releaseVersionName?.let { version ->
    val code = releaseVersionCode(version)
    // The backend gates features on the reported version, so a release never reports less than the source.
    require(code >= releaseVersionCode(sourceVersionName)) {
        "companionReleaseVersion $version is lower than the source version $sourceVersionName"
    }
    code
}

// The release key exists only in the android-release environment of a release tag run.
val releaseSigningNames = listOf("CFT_ANDROID_KEYSTORE", "ANDROID_KEYSTORE_PASSWORD", "ANDROID_KEY_ALIAS", "ANDROID_KEY_PASSWORD")
val releaseSigning: Map<String, String> = releaseSigningNames
    .associateWith { providers.environmentVariable(it).orNull.orEmpty() }
    .filterValues { it.isNotEmpty() }
if (releaseSigning.isNotEmpty() && releaseSigning.size < releaseSigningNames.size) {
    error("Release signing needs all of ${releaseSigningNames.joinToString()}; missing ${(releaseSigningNames - releaseSigning.keys).joinToString()}")
}
if (releaseVersionName != null && releaseSigning.isEmpty()) {
    error("A companionReleaseVersion build is a published release and must be signed; set ${releaseSigningNames.joinToString()}")
}

android {
    namespace = "com.alfonsusenrico.financialtracker.listener"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.alfonsusenrico.financialtracker.listener"
        minSdk = 26
        targetSdk = 35
        versionCode = publishedVersionCode ?: sourceVersionCode
        versionName = releaseVersionName ?: sourceVersionName

        testInstrumentationRunner = "com.alfonsusenrico.financialtracker.listener.VerificationTestRunner"
        vectorDrawables {
            useSupportLibrary = true
        }
    }

    signingConfigs {
        if (releaseSigning.isNotEmpty()) {
            create("release") {
                storeFile = file(releaseSigning.getValue("CFT_ANDROID_KEYSTORE"))
                storePassword = releaseSigning.getValue("ANDROID_KEYSTORE_PASSWORD")
                keyAlias = releaseSigning.getValue("ANDROID_KEY_ALIAS")
                keyPassword = releaseSigning.getValue("ANDROID_KEY_PASSWORD")
            }
        }
    }

    buildTypes {
        release {
            // Without the key the release builds unsigned, for local checks only.
            signingConfig = signingConfigs.findByName("release")
            buildConfigField("String", "SERVER_URL", "new String(\"https://finance.alfonsusenrico.com\")")
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
        debug {
            applicationIdSuffix = if (providers.gradleProperty("companionVerification").isPresent) ".verification" else ".debug"
            val serverUrl = providers.gradleProperty("companionServerUrl")
                .getOrElse("https://finance.alfonsusenrico.com").trim().removeSuffix("/")
            val parsed = URI(serverUrl)
            require(parsed.scheme in setOf("https", "http") && parsed.host != null &&
                parsed.rawUserInfo == null && parsed.rawQuery == null && parsed.rawFragment == null &&
                parsed.rawPath.isNullOrEmpty()) { "companionServerUrl must be an HTTP(S) origin" }
            // A runtime field prevents Kotlin from retaining an inlined origin when only this property changes.
            buildConfigField("String", "SERVER_URL", "new String(\"$serverUrl\")")
            isDebuggable = true
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    buildFeatures {
        compose = true
        buildConfig = true
    }

    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
    }

    sourceSets.getByName("androidTest").assets.srcDir("$projectDir/schemas")
    // Robolectric runs the Room migration and DAO tests on the JVM.
    testOptions {
        unitTests.isIncludeAndroidResources = true
    }
}

dependencies {
    // AndroidX Core & Lifecycle
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.8.5")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:2.8.5")
    implementation("androidx.activity:activity-compose:1.9.2")

    // Jetpack Compose BOM
    val composeBom = platform("androidx.compose:compose-bom:2024.08.00")
    implementation(composeBom)
    androidTestImplementation(composeBom)
    testImplementation(composeBom)
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-graphics")
    implementation("androidx.compose.ui:ui-tooling-preview")
    implementation("androidx.compose.material3:material3:1.3.0")
    implementation("androidx.compose.material:material-icons-extended")
    debugImplementation("androidx.compose.ui:ui-tooling")
    debugImplementation("androidx.compose.ui:ui-test-manifest")

    val cameraVersion = "1.4.2"
    implementation("androidx.camera:camera-camera2:$cameraVersion")
    implementation("androidx.camera:camera-lifecycle:$cameraVersion")
    implementation("androidx.camera:camera-view:$cameraVersion")
    implementation("com.google.mlkit:barcode-scanning:17.3.0")

    // Navigation Compose
    implementation("androidx.navigation:navigation-compose:2.8.0")

    // Room Database
    val roomVersion = "2.6.1"
    implementation("androidx.room:room-runtime:$roomVersion")
    implementation("androidx.room:room-ktx:$roomVersion")
    ksp("androidx.room:room-compiler:$roomVersion")

    // WorkManager
    implementation("androidx.work:work-runtime-ktx:2.9.1")

    // Networking (Retrofit + OkHttp + Gson)
    implementation("com.squareup.retrofit2:retrofit:2.11.0")
    implementation("com.squareup.retrofit2:converter-gson:2.11.0")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.squareup.okhttp3:logging-interceptor:4.12.0")
    implementation("com.google.code.gson:gson:2.11.0")

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")

    // Testing
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    testImplementation("org.robolectric:robolectric:4.14.1")
    testImplementation("androidx.test:core-ktx:1.6.1")
    testImplementation("androidx.test.ext:junit:1.2.1")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("androidx.compose.ui:ui-test-junit4")
    testImplementation("androidx.work:work-testing:2.9.1")
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.6.1")
    androidTestImplementation("androidx.compose.ui:ui-test-junit4")
    androidTestImplementation("androidx.room:room-testing:2.6.1")
    androidTestImplementation("androidx.work:work-testing:2.9.1")
    androidTestImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}
