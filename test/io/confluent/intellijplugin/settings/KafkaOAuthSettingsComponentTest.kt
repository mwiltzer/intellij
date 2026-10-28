package io.confluent.intellijplugin.settings

import com.intellij.openapi.Disposable
import com.intellij.openapi.ui.DialogPanel
import com.intellij.openapi.util.Disposer
import com.intellij.testFramework.junit5.TestApplication
import com.intellij.ui.dsl.builder.panel
import com.intellij.util.ui.UIUtil
import io.confluent.intellijplugin.core.settings.getValidator
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import javax.swing.JComponent
import javax.swing.JLabel
import javax.swing.SwingUtilities
import javax.swing.text.JTextComponent

@TestApplication
class KafkaOAuthSettingsComponentTest {

    private lateinit var disposable: Disposable
    private lateinit var component: KafkaOAuthSettingsComponent
    private lateinit var dialogPanel: DialogPanel
    private var updateCount = 0

    private val fullConfig = KafkaOAuthConfig(
        tokenEndpointUrl = "https://login.example.com/oauth/token",
        clientId = "client-id",
        clientSecret = "client-secret",
        scope = "schema-registry",
        logicalCluster = "lsrc-abc123",
        identityPoolId = "pool-xyz789",
    )

    @BeforeEach
    fun setUp() {
        disposable = Disposer.newDisposable("KafkaOAuthSettingsComponentTest")
        updateCount = 0
        component = KafkaOAuthSettingsComponent(disposable) { updateCount++ }
        onEdt { dialogPanel = panel { component.create(this) } }
    }

    @AfterEach
    fun tearDown() {
        onEdt { Disposer.dispose(disposable) }
    }

    private fun onEdt(action: () -> Unit) = SwingUtilities.invokeAndWait(action)

    // Same sequence as TestConnectionPanelWrapper: a text validator stays dormant until the first focus loss.
    private fun validationMessage(field: JComponent): String? {
        val validator = field.getValidator() ?: return null
        validator.enableValidation()
        validator.revalidate()
        return validator.validationInfo?.message
    }

    private fun textFields(): Map<String, JTextComponent> = mapOf(
        "tokenEndpointUrl" to component.tokenEndpointUrl.component,
        "clientId" to component.clientId.component,
        "clientSecret" to component.clientSecret.component,
        "scope" to component.scope.component,
        "logicalCluster" to component.logicalCluster.component,
        "identityPoolId" to component.identityPoolId.component,
    )

    @Nested
    @DisplayName("layout")
    inner class Layout {

        @Test
        fun `should render one labeled row per field`() {
            val labels = UIUtil.findComponentsOfType(dialogPanel, JLabel::class.java).map { it.text }

            assertEquals(
                listOf(
                    KafkaMessagesBundle.message("kafka.oauth.token.endpoint"),
                    KafkaMessagesBundle.message("kafka.oauth.client.id"),
                    KafkaMessagesBundle.message("kafka.oauth.client.secret"),
                    KafkaMessagesBundle.message("kafka.oauth.scope"),
                    KafkaMessagesBundle.message("kafka.oauth.logical.cluster"),
                    KafkaMessagesBundle.message("kafka.oauth.identity.pool"),
                ),
                labels
            )
        }

        @Test
        fun `should hint at the expected format of the endpoint, logical cluster and identity pool`() {
            assertEquals(
                KafkaMessagesBundle.message("kafka.oauth.token.endpoint.hint"),
                component.tokenEndpointUrl.component.emptyText.text
            )
            assertEquals(
                KafkaMessagesBundle.message("kafka.oauth.logical.cluster.hint"),
                component.logicalCluster.component.emptyText.text
            )
            assertEquals(
                KafkaMessagesBundle.message("kafka.oauth.identity.pool.hint"),
                component.identityPoolId.component.emptyText.text
            )
        }
    }

    @Nested
    @DisplayName("getConfig and applyConfig")
    inner class ConfigRoundTrip {

        @Test
        fun `should return an empty config for a fresh form`() {
            assertEquals(KafkaOAuthConfig(), component.getConfig())
        }

        @Test
        fun `should round-trip a full config`() {
            onEdt { component.applyConfig(fullConfig) }

            assertEquals(fullConfig, component.getConfig())
        }

        @Test
        fun `should replace previously applied values`() {
            onEdt { component.applyConfig(fullConfig) }
            onEdt { component.applyConfig(KafkaOAuthConfig(clientId = "other")) }

            assertEquals(KafkaOAuthConfig(clientId = "other"), component.getConfig())
        }
    }

    @Nested
    @DisplayName("onUpdate")
    inner class OnUpdate {

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = ["tokenEndpointUrl", "clientId", "clientSecret", "scope", "logicalCluster", "identityPoolId"])
        fun `should notify when the field changes`(field: String) {
            val textField = textFields().getValue(field)

            onEdt { textField.text = "changed" }

            assertEquals(1, updateCount)
        }
    }

    @Nested
    @DisplayName("validation")
    inner class Validation {

        private val notEmpty = KafkaMessagesBundle.message("validator.notEmpty")
        private val invalidEndpoint = KafkaMessagesBundle.message("kafka.oauth.token.endpoint.invalid")

        @Test
        fun `should require a token endpoint`() {
            assertEquals(notEmpty, validationMessage(component.tokenEndpointUrl.component))
        }

        @ParameterizedTest(name = "[{0}]")
        @ValueSource(
            strings = [
                "login.example.com/oauth/token",
                "ftp://login.example.com/oauth/token",
                "https://",
                "http:///oauth/token",
                "http://login example.com/token",
                "not a url",
            ]
        )
        fun `should reject a token endpoint that is not an http(s) URL`(url: String) {
            onEdt { component.tokenEndpointUrl.component.text = url }

            assertEquals(invalidEndpoint, validationMessage(component.tokenEndpointUrl.component))
        }

        @ParameterizedTest(name = "[{0}]")
        @ValueSource(
            strings = [
                "https://login.example.com/oauth/token",
                "http://localhost:8080/oauth/token",
                "HTTPS://LOGIN.EXAMPLE.COM/oauth/token",
                "  https://login.example.com/oauth/token  ",
            ]
        )
        fun `should accept an http or https token endpoint`(url: String) {
            onEdt { component.tokenEndpointUrl.component.text = url }

            assertNull(validationMessage(component.tokenEndpointUrl.component))
        }

        @Test
        fun `should require a client id`() {
            assertEquals(notEmpty, validationMessage(component.clientId.component))

            onEdt { component.clientId.component.text = "client-id" }

            assertNull(validationMessage(component.clientId.component))
        }

        @Test
        fun `should require a client secret`() {
            assertEquals(notEmpty, validationMessage(component.clientSecret.component))

            onEdt { component.clientSecret.component.text = "client-secret" }

            assertNull(validationMessage(component.clientSecret.component))
        }

        @Test
        fun `should not validate the optional fields`() {
            assertNotNull(component.clientId.component.getValidator())
            assertNull(component.scope.component.getValidator())
            assertNull(component.logicalCluster.component.getValidator())
            assertNull(component.identityPoolId.component.getValidator())
        }
    }
}
