import java.net.URI

plugins {
    alias(libs.plugins.kotlin.multiplatform)

    id("maven-publish")
}

group = "dk.spilpind"
version = "0.1.0-dev"
val baseArtifactId = "sms-client"

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
