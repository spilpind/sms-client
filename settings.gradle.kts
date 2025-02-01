import java.io.FileInputStream
import java.util.*

rootProject.name = "sms-client"

pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()

        maven {
            url = uri("https://maven.pkg.github.com/spilpind/sms-core")

            credentials {
                setGithubCredentials()
            }
        }
        maven {
            url = uri("https://maven.pkg.github.com/anigif/anigif-kmp")

            credentials {
                setGithubCredentials()
            }
        }
    }
}

include(":library")


private fun PasswordCredentials.setGithubCredentials() {
    val propertyFile = File(rootProject.projectDir, "local.properties")
    val properties = Properties()
    if (propertyFile.exists()) {
        val inputStream = FileInputStream(propertyFile)
        properties.load(inputStream)
    }

    val propertyUsernameKey = "github.username"
    val propertyUsername: String? = properties.getProperty(propertyUsernameKey)
    val propertyPasswordKey = "github.token"
    val propertyPassword: String? = properties.getProperty(propertyPasswordKey)

    if (propertyUsername != null || propertyPassword != null) {
        if (propertyUsername == null) {
            throw IllegalArgumentException("When setting $propertyPasswordKey, $propertyUsernameKey should be set as well")
        } else if (propertyPassword == null) {
            throw IllegalArgumentException("When setting $propertyUsernameKey, $propertyPasswordKey should be set as well")
        }

        username = propertyUsername
        password = propertyPassword
        return
    }

    val environmentUsernameKey = "GITHUB_ACTOR"
    val environmentUsername: String? = System.getenv(environmentUsernameKey)
    val environmentPasswordKey = "GITHUB_TOKEN"
    val environmentPassword: String? = System.getenv(environmentPasswordKey)

    if (environmentUsername != null || environmentPassword != null) {
        if (environmentUsername == null) {
            throw IllegalArgumentException("When setting $environmentPasswordKey, $environmentUsernameKey should be set as well")
        } else if (environmentPassword == null) {
            throw IllegalArgumentException("When setting $environmentUsernameKey, $environmentPasswordKey should be set as well")
        }

        username = environmentUsername
        password = environmentPassword
        return
    }

    throw IllegalArgumentException(
        "In order to use Github packages, " +
            "$propertyUsernameKey/$propertyPasswordKey needs to be set in ${propertyFile.path} " +
            "or $environmentUsernameKey/$environmentPasswordKey needs to be set as environment variables"
    )
}
