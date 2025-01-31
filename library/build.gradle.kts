plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

group = "dk.spilpind"
version = "0.1.0"

kotlin {
    jvm()

    iosArm64() // iOS device
    iosSimulatorArm64() // iOS simulator (on Apple silicon machine)
    iosX64() // iOS simulator (on Intel machine)

    sourceSets {
        val commonMain by getting {
            dependencies {
                // SMS
                implementation(libs.sms.core)

                // Kotlin
                implementation(libs.kotlin.coroutines.core)
                implementation(libs.kotlin.datetime)

                // Ktor
                implementation(libs.ktor.client.content.negotiation)
                implementation(libs.ktor.client.websockets)
                implementation(libs.ktor.serialization.json)

                // Misc
                implementation(libs.anigif.kmp)
                implementation(libs.kermit)
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(libs.kotlin.test)
                implementation(libs.kotlin.test.coroutines)
                implementation(libs.ktor.test.client.mock)
            }
        }
    }
}
