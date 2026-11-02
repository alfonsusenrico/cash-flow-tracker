# Proguard rules for Financial Tracker Mobile Listener
-keepclassmembers class * {
    @androidx.room.* <methods>;
}
-keepattributes Signature
-keepattributes *Annotation*
