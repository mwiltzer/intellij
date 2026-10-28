package io.confluent.intellijplugin.settings

import com.intellij.openapi.Disposable
import com.intellij.openapi.project.ProjectManager
import com.intellij.openapi.util.Disposer
import com.intellij.testFramework.junit5.TestApplication
import com.intellij.ui.dsl.builder.panel
import com.intellij.util.ui.UIUtil
import io.confluent.intellijplugin.core.settings.ModificationKey
import io.confluent.intellijplugin.core.settings.fields.RadioGroupField
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.rfs.KafkaConfigurationSource
import io.confluent.intellijplugin.rfs.KafkaConnectionData
import io.confluent.intellijplugin.rfs.SchemaRegistryAuthType
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import javax.swing.JRadioButton
import javax.swing.SwingUtilities

@TestApplication
class KafkaRegistrySettingsOAuthTest {

    private val project = ProjectManager.getInstance().defaultProject
    private lateinit var disposable: Disposable
    private lateinit var coroutineScope: CoroutineScope

    private val oauthConfig = KafkaOAuthConfig(
        tokenEndpointUrl = "https://login.example.com/oauth/token",
        clientId = "client-id",
        clientSecret = "client-secret",
        scope = "schema-registry",
        logicalCluster = "lsrc-abc123",
        identityPoolId = "pool-xyz789",
    )

    @BeforeEach
    fun setUp() {
        disposable = Disposer.newDisposable("KafkaRegistrySettingsOAuthTest")
        coroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    }

    @AfterEach
    fun tearDown() {
        coroutineScope.cancel()
        onEdt { Disposer.dispose(disposable) }
    }

    private fun <T> onEdt(action: () -> T): T {
        var result: T? = null
        SwingUtilities.invokeAndWait { result = action() }
        @Suppress("UNCHECKED_CAST")
        return result as T
    }

    private fun createSettings(): KafkaRegistrySettings {
        val connectionData = KafkaConnectionData().apply { registryType = KafkaRegistryType.CONFLUENT }
        val registryType = RadioGroupField(
            KafkaConnectionData::registryType,
            ModificationKey("Schema Registry type"),
            connectionData,
            KafkaRegistryType.entries
        )
        val settings = onEdt {
            KafkaRegistrySettings(project, connectionData, disposable, coroutineScope, registryType)
                .also { panel { it.setPanelComponent(this) } }
        }
        awaitCredentialsLoaded(settings)
        return settings
    }

    // The properties editor is disabled and filled with a placeholder until the stored credentials are loaded.
    private fun awaitCredentialsLoaded(settings: KafkaRegistrySettings) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (!settings.confluentPropertiesEditor.getComponent().isEnabled) {
            check(System.nanoTime() < deadline) { "Registry credentials were not loaded in time" }
            onEdt { }
            Thread.sleep(10)
        }
    }

    private fun KafkaRegistrySettings.selectAuth(type: SchemaRegistryAuthType) =
        onEdt { confluentSchemaAuth.selectedItem = type }

    // Clicks like a user would: RadioGroupField.setValue() corrupts the action command of the selected button.
    private fun KafkaRegistrySettings.selectSource(source: KafkaConfigurationSource) = onEdt {
        UIUtil.findComponentsOfType(confluentSource.getComponent(), JRadioButton::class.java)
            .first { it.actionCommand == source.id }
            .doClick()
    }

    private fun KafkaRegistrySettings.fillOAuthForm(config: KafkaOAuthConfig) = onEdt {
        confluentOAuthComponent.tokenEndpointUrl.component.text = config.tokenEndpointUrl
        confluentOAuthComponent.clientId.component.text = config.clientId
        confluentOAuthComponent.clientSecret.component.text = config.clientSecret
        confluentOAuthComponent.scope.component.text = config.scope
        confluentOAuthComponent.logicalCluster.component.text = config.logicalCluster
        confluentOAuthComponent.identityPoolId.component.text = config.identityPoolId
    }

    private fun KafkaRegistrySettings.pasteProperties(text: String) =
        onEdt { confluentPropertiesEditor.getComponent().text = text }

    private fun KafkaRegistrySettings.properties(): Map<String, String> =
        onEdt { confluentPropertiesEditor.getProperties() } ?: error("Registry properties are not parseable")

    private fun KafkaRegistrySettings.authProperties(): Map<String, String> =
        properties().filterKeys { it.startsWith("bearer.auth.") || it.startsWith("basic.auth.") || it == "user.info" }

    private fun KafkaRegistrySettings.oauthFormVisible() = onEdt { confluentOAuthComponent.tokenEndpointUrl.component.isVisible }

    private fun KafkaRegistrySettings.basicFormVisible() = onEdt { confluentBasicLogin.component.isVisible }

    private fun KafkaRegistrySettings.bearerFormVisible() = onEdt { confluentBearerToken.component.isVisible }

    @Nested
    @DisplayName("authentication type")
    inner class AuthenticationType {

        @Test
        fun `should offer OAuth 2_0 after the other authentication types`() {
            assertEquals(SchemaRegistryAuthType.OAUTH, SchemaRegistryAuthType.entries.last())
        }

        @Test
        fun `should show only the OAuth form when OAuth is selected`() {
            val settings = createSettings()

            settings.selectAuth(SchemaRegistryAuthType.OAUTH)

            assertTrue(settings.oauthFormVisible())
            assertFalse(settings.basicFormVisible())
            assertFalse(settings.bearerFormVisible())
        }

        @Test
        fun `should hide the OAuth form for the other authentication types`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)

            SchemaRegistryAuthType.entries.filter { it != SchemaRegistryAuthType.OAUTH }.forEach {
                settings.selectAuth(it)

                assertFalse(settings.oauthFormVisible(), "OAuth form must be hidden for $it")
            }
        }

        @Test
        fun `should hide the OAuth form while the properties source is used`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)

            settings.selectSource(KafkaConfigurationSource.FROM_PROPERTIES)
            assertFalse(settings.oauthFormVisible())

            settings.selectSource(KafkaConfigurationSource.FROM_UI)
            assertTrue(settings.oauthFormVisible())
        }
    }

    @Nested
    @DisplayName("form to properties")
    inner class FormToProperties {

        @Test
        fun `should write the OAuth credentials source as soon as OAuth is selected`() {
            val settings = createSettings()

            settings.selectAuth(SchemaRegistryAuthType.OAUTH)

            assertEquals(
                mapOf(SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE to "OAUTHBEARER"),
                settings.authProperties()
            )
        }

        @Test
        fun `should write every filled OAuth field`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)

            settings.fillOAuthForm(oauthConfig)

            assertEquals(
                oauthConfig.toRegistryProperties().filterValues { it != null },
                settings.authProperties()
            )
        }

        @Test
        fun `should remove the key of a cleared optional field`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)
            settings.fillOAuthForm(oauthConfig)

            settings.fillOAuthForm(oauthConfig.copy(scope = ""))

            assertFalse(settings.authProperties().containsKey(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE))
        }

        @Test
        fun `should keep the other registry properties untouched`() {
            val settings = createSettings()
            settings.pasteProperties("schema.registry.url=https://psrc.example.com\ncustom.key=custom-value\n")
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)

            settings.fillOAuthForm(oauthConfig)

            val properties = settings.properties()
            assertEquals("https://psrc.example.com", properties["schema.registry.url"])
            assertEquals("custom-value", properties["custom.key"])
        }
    }

    @Nested
    @DisplayName("properties to form")
    inner class PropertiesToForm {

        @Test
        fun `should select OAuth and fill the form from pasted properties`() {
            val settings = createSettings()

            settings.pasteProperties(
                oauthConfig.toRegistryProperties().entries.joinToString("\n", postfix = "\n") { "${it.key}=${it.value}" }
            )

            assertEquals(SchemaRegistryAuthType.OAUTH, onEdt { settings.confluentSchemaAuth.selectedItem })
            assertEquals(oauthConfig, onEdt { settings.confluentOAuthComponent.getConfig() })
            assertTrue(settings.oauthFormVisible())
        }

        @Test
        fun `should select OAuth when only the credentials source is present`() {
            val settings = createSettings()

            settings.pasteProperties("bearer.auth.credentials.source=OAUTHBEARER\n")

            assertEquals(SchemaRegistryAuthType.OAUTH, onEdt { settings.confluentSchemaAuth.selectedItem })
            assertEquals(KafkaOAuthConfig(), onEdt { settings.confluentOAuthComponent.getConfig() })
        }

        @Test
        fun `should still map a static bearer token to the bearer form`() {
            val settings = createSettings()

            settings.pasteProperties("bearer.auth.credentials.source=STATIC_TOKEN\nbearer.auth.token=static-token\n")

            assertEquals(SchemaRegistryAuthType.BEARER, onEdt { settings.confluentSchemaAuth.selectedItem })
            assertEquals("static-token", onEdt { settings.confluentBearerToken.component.text })
            assertEquals(KafkaOAuthConfig(), onEdt { settings.confluentOAuthComponent.getConfig() })
        }

        @Test
        fun `should still map basic auth to the basic form`() {
            val settings = createSettings()

            settings.pasteProperties("basic.auth.credentials.source=USER_INFO\nbasic.auth.user.info=user:secret\n")

            assertEquals(SchemaRegistryAuthType.BASIC_AUTH, onEdt { settings.confluentSchemaAuth.selectedItem })
            assertEquals("user", onEdt { settings.confluentBasicLogin.component.text })
        }

        @Test
        fun `should restore the form from the properties it generated`() {
            val first = createSettings()
            first.selectAuth(SchemaRegistryAuthType.OAUTH)
            first.fillOAuthForm(oauthConfig)
            val generated = onEdt { first.confluentPropertiesEditor.getComponent().text }

            val second = createSettings()
            second.pasteProperties(generated)

            assertEquals(SchemaRegistryAuthType.OAUTH, onEdt { second.confluentSchemaAuth.selectedItem })
            assertEquals(oauthConfig, onEdt { second.confluentOAuthComponent.getConfig() })
        }
    }

    @Nested
    @DisplayName("switching the authentication type")
    inner class SwitchingAuthenticationType {

        private val oauthKeys = KafkaOAuthConfig.MANAGED_KEYS + SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE

        @Test
        fun `should drop every OAuth key when switching to basic auth`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)
            settings.fillOAuthForm(oauthConfig)

            settings.selectAuth(SchemaRegistryAuthType.BASIC_AUTH)

            val authProperties = settings.authProperties()
            assertTrue(authProperties.keys.none { it in oauthKeys }, "Unexpected OAuth keys in $authProperties")
            assertEquals("USER_INFO", authProperties[SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE])
        }

        @Test
        fun `should drop every OAuth key when switching to a static bearer token`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)
            settings.fillOAuthForm(oauthConfig)

            settings.selectAuth(SchemaRegistryAuthType.BEARER)

            val authProperties = settings.authProperties()
            assertTrue(authProperties.keys.none { it in KafkaOAuthConfig.MANAGED_KEYS }, "Unexpected OAuth keys in $authProperties")
            assertEquals("STATIC_TOKEN", authProperties[SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE])
        }

        @Test
        fun `should drop every authentication key when switching to none`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)
            settings.fillOAuthForm(oauthConfig)

            settings.selectAuth(SchemaRegistryAuthType.NOT_SPECIFIED)

            assertEquals(emptyMap<String, String>(), settings.authProperties())
        }

        @Test
        fun `should bring the OAuth keys back when switching back to OAuth`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)
            settings.fillOAuthForm(oauthConfig)
            settings.selectAuth(SchemaRegistryAuthType.NOT_SPECIFIED)

            settings.selectAuth(SchemaRegistryAuthType.OAUTH)

            assertEquals(
                oauthConfig.toRegistryProperties().filterValues { it != null },
                settings.authProperties()
            )
        }
    }

    @Nested
    @DisplayName("SSL settings")
    inner class SslSettings {

        @Test
        fun `should write custom truststore settings next to the OAuth keys`() {
            val settings = createSettings()
            settings.selectAuth(SchemaRegistryAuthType.OAUTH)
            settings.fillOAuthForm(oauthConfig)

            onEdt {
                settings.confluentUseBrokerSsl.component.doClick()
                settings.confluentSslComponent.sslTruststoreLocation.component.text = "/tmp/truststore.jks"
            }

            val properties = settings.properties()
            assertEquals("/tmp/truststore.jks", properties["schema.registry.ssl.truststore.location"])
            assertEquals(oauthConfig.tokenEndpointUrl, properties[SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL])
        }
    }
}
