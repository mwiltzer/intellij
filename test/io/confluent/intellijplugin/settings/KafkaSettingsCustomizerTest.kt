package io.confluent.intellijplugin.settings

import com.intellij.openapi.Disposable
import com.intellij.openapi.project.ProjectManager
import com.intellij.openapi.util.Disposer
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.rfs.KafkaConnectionData
import io.confluent.intellijplugin.rfs.SchemaRegistryAuthType
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
}
