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
                // multiplatform dependencies
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(libs.kotlin.test)
            }
        }
    }
}
