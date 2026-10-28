package io.confluent.intellijplugin.settings

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig

/**
 * OAuth 2.0 client-credentials settings for a Confluent Schema Registry connection, mapped to and from the
 * `bearer.auth.*` properties understood by the Schema Registry client's `OAUTHBEARER` credential provider.
 */
data class KafkaOAuthConfig(
    val tokenEndpointUrl: String = "",
    val clientId: String = "",
    val clientSecret: String = "",
    val scope: String = "",
    val logicalCluster: String = "",
    val identityPoolId: String = "",
) {
    /**
     * Registry properties for this configuration. Blank values map to `null` so that the corresponding keys
     * are removed from the registry properties editor.
     */
    fun toRegistryProperties(): Map<String, String?> = mapOf(
        SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE to OAUTHBEARER_CREDENTIALS_SOURCE,
        SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL to tokenEndpointUrl.trim().ifBlank { null },
        SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID to clientId.trim().ifBlank { null },
        SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET to clientSecret.ifBlank { null },
        SchemaRegistryClientConfig.BEARER_AUTH_SCOPE to scope.trim().ifBlank { null },
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER to logicalCluster.trim().ifBlank { null },
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID to identityPoolId.trim().ifBlank { null },
    )

    companion object {
        const val OAUTHBEARER_CREDENTIALS_SOURCE = "OAUTHBEARER"

        /** The `bearer.auth.*` keys owned by the OAuth form (the credentials source key is shared with the static bearer token). */
        val MANAGED_KEYS: List<String> = listOf(
            SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL,
            SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID,
            SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET,
            SchemaRegistryClientConfig.BEARER_AUTH_SCOPE,
            SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER,
            SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID,
        )

        fun isOAuth(properties: Map<String, String>): Boolean =
            properties[SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE] == OAUTHBEARER_CREDENTIALS_SOURCE

        fun fromRegistryProperties(properties: Map<String, String>) = KafkaOAuthConfig(
            tokenEndpointUrl = properties[SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL] ?: "",
            clientId = properties[SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID] ?: "",
            clientSecret = properties[SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET] ?: "",
            scope = properties[SchemaRegistryClientConfig.BEARER_AUTH_SCOPE] ?: "",
            logicalCluster = properties[SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER] ?: "",
            identityPoolId = properties[SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID] ?: "",
        )
    }
}
