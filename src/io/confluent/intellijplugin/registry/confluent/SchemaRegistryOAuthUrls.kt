package io.confluent.intellijplugin.registry.confluent

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig

/**
 * Keeps the JVM allow-list of OAuth token endpoints in sync with the configured Schema Registry connections.
 *
 * Since the CVE-2025-27817 hardening, kafka-clients refuses to contact any OAuth issuer endpoint that is not listed
 * in the `org.apache.kafka.sasl.oauthbearer.allowed.urls` system property. The Schema Registry client's `OAUTHBEARER`
 * credential provider validates `bearer.auth.issuer.endpoint.url` against that very list, so the endpoint has to be
 * registered before the registry client or any Schema-Registry-backed serializer is configured.
 */
object SchemaRegistryOAuthUrls {
    const val ALLOWED_URLS_PROPERTY = "org.apache.kafka.sasl.oauthbearer.allowed.urls"

    fun allowTokenEndpoint(properties: Map<*, *>) =
        allowTokenEndpoint(properties[SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL]?.toString())

    @Synchronized
    fun allowTokenEndpoint(tokenEndpointUrl: String?) {
        val url = tokenEndpointUrl?.trim()?.ifEmpty { null } ?: return
        val allowed = allowedUrls()
        if (url in allowed) return
        System.setProperty(ALLOWED_URLS_PROPERTY, (allowed + url).joinToString(","))
    }

    fun allowedUrls(): List<String> =
        System.getProperty(ALLOWED_URLS_PROPERTY, "")
            .split(",")
            .map { it.trim() }
            .filter { it.isNotEmpty() }
}
