package io.confluent.intellijplugin.settings

import com.intellij.openapi.Disposable
import com.intellij.ui.components.JBPasswordField
import com.intellij.ui.components.JBTextField
import com.intellij.ui.dsl.builder.*
import io.confluent.intellijplugin.core.settings.withNonEmptyValidator
import io.confluent.intellijplugin.core.settings.withValidator
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import java.net.URI

class KafkaOAuthSettingsComponent(private val uiDisposable: Disposable, private val onUpdate: () -> Unit) {
    internal lateinit var tokenEndpointUrl: Cell<JBTextField>
    internal lateinit var clientId: Cell<JBTextField>
    internal lateinit var clientSecret: Cell<JBPasswordField>
    internal lateinit var scope: Cell<JBTextField>
    internal lateinit var logicalCluster: Cell<JBTextField>
    internal lateinit var identityPoolId: Cell<JBTextField>

    fun create(panel: Panel) = panel.rowsRange {
        row(KafkaMessagesBundle.message("kafka.oauth.token.endpoint")) {
            tokenEndpointUrl = textField().align(AlignX.FILL).onChanged { onUpdate() }.apply {
                component.emptyText.text = KafkaMessagesBundle.message("kafka.oauth.token.endpoint.hint")
                component.withValidator(uiDisposable, ::validateTokenEndpoint)
            }
        }
        row(KafkaMessagesBundle.message("kafka.oauth.client.id")) {
            clientId = textField().align(AlignX.FILL).onChanged { onUpdate() }.apply {
                component.withNonEmptyValidator(uiDisposable)
            }
        }
        row(KafkaMessagesBundle.message("kafka.oauth.client.secret")) {
            clientSecret = passwordField().align(AlignX.FILL).onChanged { onUpdate() }.apply {
                component.withNonEmptyValidator(uiDisposable)
            }
        }
        row(KafkaMessagesBundle.message("kafka.oauth.scope")) {
            scope = textField().align(AlignX.FILL).onChanged { onUpdate() }
        }
        row(KafkaMessagesBundle.message("kafka.oauth.logical.cluster")) {
            logicalCluster = textField().align(AlignX.FILL).onChanged { onUpdate() }.apply {
                component.emptyText.text = KafkaMessagesBundle.message("kafka.oauth.logical.cluster.hint")
            }
        }
        row(KafkaMessagesBundle.message("kafka.oauth.identity.pool")) {
            identityPoolId = textField().align(AlignX.FILL).onChanged { onUpdate() }.apply {
                component.emptyText.text = KafkaMessagesBundle.message("kafka.oauth.identity.pool.hint")
            }
        }
    }

    fun applyConfig(config: KafkaOAuthConfig) {
        tokenEndpointUrl.text(config.tokenEndpointUrl)
        clientId.text(config.clientId)
        clientSecret.text(config.clientSecret)
        scope.text(config.scope)
        logicalCluster.text(config.logicalCluster)
        identityPoolId.text(config.identityPoolId)
    }

    fun getConfig() = KafkaOAuthConfig(
        tokenEndpointUrl = tokenEndpointUrl.component.text,
        clientId = clientId.component.text,
        clientSecret = String(clientSecret.component.password),
        scope = scope.component.text,
        logicalCluster = logicalCluster.component.text,
        identityPoolId = identityPoolId.component.text,
    )

    private fun validateTokenEndpoint(text: String): String? {
        if (text.isBlank()) return KafkaMessagesBundle.message("validator.notEmpty")

        val uri = runCatching { URI(text.trim()) }.getOrNull()
        val isHttpUrl = uri != null && uri.scheme?.lowercase() in HTTP_SCHEMES && !uri.host.isNullOrEmpty()
        return if (isHttpUrl) null else KafkaMessagesBundle.message("kafka.oauth.token.endpoint.invalid")
    }

    private companion object {
        val HTTP_SCHEMES = setOf("http", "https")
    }
}
