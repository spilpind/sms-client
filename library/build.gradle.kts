import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.dsl.KotlinMultiplatformExtension
import java.net.URI

plugins {
    alias(libs.plugins.kotlin.multiplatform)

    id("maven-publish")
}

group = "dk.spilpind"
version = "0.1.0-dev"
val baseArtifactId = "sms-client"

kotlin {
    setupTargets()

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

    compilerOptions {
        val allWarningsAsErrorsArgument = "allWarningsAsErrors"
        if (project.hasProperty(allWarningsAsErrorsArgument)) {
            allWarningsAsErrors = (project.property(allWarningsAsErrorsArgument) == "true")
        }
    }
}

publishing {
    publications {
        publications.withType<MavenPublication> {
            if (artifactId.startsWith(project.name, ignoreCase = true)) {
                artifactId = artifactId.replaceFirst(project.name, baseArtifactId)
            }
        }
    }

    repositories {
        maven {
            name = "GitHubPackages"
            url = URI("https://maven.pkg.github.com/anigif/anigif-kmp")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}

private fun KotlinMultiplatformExtension.setupTargets() {
    // We try to aim for as many targets as possible as we don't have any platform-specific code and just want to be as
    // available as possible. The targets were however limited by ktor and thus we for instance don't support as many as
    // sms-core. See supported targets by ktor here: https://ktor.io/docs/client-supported-platforms.html

    jvm()
    jvmToolchain(17)

    iosArm64() // iOS device
    iosSimulatorArm64() // iOS simulator (on Apple Silicon machine)
    iosX64() // iOS simulator (on Intel machine)

    linuxX64()
    mingwX64() // 64-bit Windows 7 and later using MinGW compatibility layer
    macosX64()
    macosArm64() // Apple macOS (on Apple Silicon machine)

    watchosArm32() // Apple watchOS on ARM32 platforms
    watchosArm64() // Apple watchOS on ARM64 platforms with ILP32
    watchosSimulatorArm64() // Apple watchOS simulator (on Apple Silicon machine)
    watchosX64() // Apple watchOS 64-bit simulator (on Intel machine)

    tvosArm64() // Apple tvOS on ARM64 platforms
    tvosSimulatorArm64() // Apple tvOS simulator (on Apple Silicon machine)
    tvosX64() // Apple tvOS simulator (on Intel machine)

    js {
        nodejs()
    }
}
