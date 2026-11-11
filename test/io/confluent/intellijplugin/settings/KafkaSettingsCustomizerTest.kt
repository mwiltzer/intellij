package io.confluent.intellijplugin.settings

import com.intellij.openapi.Disposable
import com.intellij.openapi.project.ProjectManager
import com.intellij.openapi.util.Disposer
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.core.settings.isValidateable
import io.confluent.intellijplugin.core.settings.labelToName
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.rfs.KafkaConfigurationSource
import io.confluent.intellijplugin.rfs.KafkaConnectionData
import io.confluent.intellijplugin.rfs.SchemaRegistryAuthType
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import javax.swing.SwingUtilities

@TestApplication
class KafkaSettingsCustomizerTest {

    private val project = ProjectManager.getInstance().defaultProject
    private lateinit var disposable: Disposable
    private lateinit var coroutineScope: CoroutineScope
    private lateinit var customizer: KafkaSettingsCustomizer

    @BeforeEach
    fun setUp() {
        disposable = Disposer.newDisposable("KafkaSettingsCustomizerTest")
        coroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val connectionData = KafkaConnectionData()
        onEdt {
            customizer = KafkaSettingsCustomizer(project, connectionData, disposable, coroutineScope)
            customizer.getDefaultComponent(customizer.getDefaultFields(), connectionData)
        }
        awaitCredentialsLoaded()
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

    // The properties editor stays disabled until the stored credentials are loaded.
    private fun awaitCredentialsLoaded() {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (!customizer.registryConfluentProperties.getComponent().isEnabled) {
            check(System.nanoTime() < deadline) { "Registry credentials were not loaded in time" }
            onEdt { }
            Thread.sleep(10)
        }
    }

    @Nested
    @DisplayName("Schema Registry OAuth fields")
    inner class RegistryOAuthFields {

        @Test
        fun `should map every exposed field to its own registry property`() {
            onEdt {
                customizer.registryConfluentAuth.selectedItem = SchemaRegistryAuthType.OAUTH
                customizer.registryConfluentOAuthTokenEndpointUrl.component.text = "https://login.example.com/oauth/token"
                customizer.registryConfluentOAuthClientId.component.text = "client-id"
                customizer.registryConfluentOAuthClientSecret.component.text = "client-secret"
                customizer.registryConfluentOAuthScope.component.text = "schema-registry"
                customizer.registryConfluentOAuthLogicalCluster.component.text = "lsrc-abc123"
                customizer.registryConfluentOAuthIdentityPoolId.component.text = "pool-xyz789"
            }

            val properties = onEdt { customizer.registryConfluentProperties.getProperties() }.orEmpty()

            assertEquals(
                mapOf(
                    SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE to "OAUTHBEARER",
                    SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL to "https://login.example.com/oauth/token",
                    SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID to "client-id",
                    SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET to "client-secret",
                    SchemaRegistryClientConfig.BEARER_AUTH_SCOPE to "schema-registry",
                    SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER to "lsrc-abc123",
                    SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID to "pool-xyz789",
                ),
                properties.filterKeys { it.startsWith("bearer.auth.") }
            )
        }
    }

    @Nested
    @DisplayName("Schema Registry OAuth validation before the connection test")
    inner class RegistryOAuthValidation {

        private val tokenEndpointLabel = labelOf("kafka.oauth.token.endpoint")
        private val clientIdLabel = labelOf("kafka.oauth.client.id")
        private val clientSecretLabel = labelOf("kafka.oauth.client.secret")
        private val notEmpty = KafkaMessagesBundle.message("validator.notEmpty")

        @BeforeEach
        fun useConfluentRegistryWithOAuth() {
            val connectionData = KafkaConnectionData().apply { registryType = KafkaRegistryType.CONFLUENT }
            onEdt {
                customizer = KafkaSettingsCustomizer(project, connectionData, disposable, coroutineScope)
                customizer.getDefaultComponent(customizer.getDefaultFields(), connectionData)
            }
            awaitCredentialsLoaded()
            selectAuthentication(SchemaRegistryAuthType.OAUTH)
        }

        private fun labelOf(key: String) = labelToName(KafkaMessagesBundle.message(key))

        private fun selectAuthentication(type: SchemaRegistryAuthType) =
            onEdt { customizer.registryConfluentAuth.selectedItem = type }

        private fun fillOAuthFields(tokenEndpoint: String = "https://login.example.com/oauth/token") = onEdt {
            customizer.registryConfluentOAuthTokenEndpointUrl.component.text = tokenEndpoint
            customizer.registryConfluentOAuthClientId.component.text = "client-id"
            customizer.registryConfluentOAuthClientSecret.component.text = "client-secret"
        }

        // The validation that TestConnectionPanelWrapper runs on all the fields before it tests the connection.
        private fun oauthErrorsBeforeTestingConnection(): List<Pair<String, String>> = onEdt {
            customizer.getDefaultFields().flatMap { field ->
                if (!isValidateable(field.getComponent())) return@flatMap emptyList()
                field.getValidators().mapNotNull { validator ->
                    validator.enableValidation()
                    validator.revalidate()
                    validator.validationInfo?.takeIf { !it.warning }?.let { field.key.label to it.message }
                }
            }
        }.filter { (label, _) -> label in setOf(tokenEndpointLabel, clientIdLabel, clientSecretLabel) }

        @Test
        fun `should name the OAuth fields that are missing`() {
            assertEquals(
                listOf(tokenEndpointLabel to notEmpty, clientIdLabel to notEmpty, clientSecretLabel to notEmpty),
                oauthErrorsBeforeTestingConnection()
            )
        }

        @Test
        fun `should name a token endpoint that is not a url`() {
            fillOAuthFields(tokenEndpoint = "not a url")

            assertEquals(
                listOf(tokenEndpointLabel to KafkaMessagesBundle.message("kafka.oauth.token.endpoint.invalid")),
                oauthErrorsBeforeTestingConnection()
            )
        }

        @Test
        fun `should pass complete OAuth settings`() {
            fillOAuthFields()

            assertEquals(emptyList<Pair<String, String>>(), oauthErrorsBeforeTestingConnection())
        }

        @Test
        fun `should include the OAuth fields in the errors that stop the connection test`() {
            oauthErrorsBeforeTestingConnection()

            val components = setOf(
                customizer.registryConfluentOAuthTokenEndpointUrl.component,
                customizer.registryConfluentOAuthClientId.component,
                customizer.registryConfluentOAuthClientSecret.component,
            )

            val failing = onEdt { customizer.getValidationErrors() }.mapNotNull { it.component }.filter { it in components }
            assertEquals(components, failing.toSet())
        }

        @Test
        fun `should not validate the OAuth fields for another authentication`() {
            selectAuthentication(SchemaRegistryAuthType.BASIC_AUTH)

            assertEquals(emptyList<Pair<String, String>>(), oauthErrorsBeforeTestingConnection())
        }

        @Test
        fun `should not validate the OAuth fields when the properties are edited directly`() {
            onEdt { customizer.registryConfluentSource.setValue(KafkaConfigurationSource.FROM_PROPERTIES) }

            assertEquals(emptyList<Pair<String, String>>(), oauthErrorsBeforeTestingConnection())
        }

        @Test
        fun `should not validate the OAuth fields for another kind of registry`() {
            onEdt { customizer.registryType.setValue(KafkaRegistryType.AWS_GLUE) }

            assertEquals(emptyList<Pair<String, String>>(), oauthErrorsBeforeTestingConnection())
        }
    }
}
