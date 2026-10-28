package io.confluent.intellijplugin.registry.confluent

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Properties

class SchemaRegistryOAuthUrlsTest {

    private var originalValue: String? = null

    @BeforeEach
    fun saveSystemProperty() {
        originalValue = System.getProperty(SchemaRegistryOAuthUrls.ALLOWED_URLS_PROPERTY)
        System.clearProperty(SchemaRegistryOAuthUrls.ALLOWED_URLS_PROPERTY)
    }

    @AfterEach
    fun restoreSystemProperty() {
        originalValue?.let { System.setProperty(SchemaRegistryOAuthUrls.ALLOWED_URLS_PROPERTY, it) }
            ?: System.clearProperty(SchemaRegistryOAuthUrls.ALLOWED_URLS_PROPERTY)
    }

    @Test
    fun `should register the token endpoint when the allow-list is empty`() {
        SchemaRegistryOAuthUrls.allowTokenEndpoint("https://login.example.com/oauth/token")

        assertEquals(listOf("https://login.example.com/oauth/token"), SchemaRegistryOAuthUrls.allowedUrls())
    }

    @Test
    fun `should keep previously allowed urls`() {
        System.setProperty(SchemaRegistryOAuthUrls.ALLOWED_URLS_PROPERTY, "https://idp-a.example.com/token, https://idp-b.example.com/token")

        SchemaRegistryOAuthUrls.allowTokenEndpoint("https://idp-c.example.com/token")

        assertEquals(
            listOf("https://idp-a.example.com/token", "https://idp-b.example.com/token", "https://idp-c.example.com/token"),
            SchemaRegistryOAuthUrls.allowedUrls()
        )
    }

    @Test
    fun `should not duplicate an already allowed url`() {
        SchemaRegistryOAuthUrls.allowTokenEndpoint("https://login.example.com/oauth/token")
        SchemaRegistryOAuthUrls.allowTokenEndpoint("  https://login.example.com/oauth/token  ")

        assertEquals(listOf("https://login.example.com/oauth/token"), SchemaRegistryOAuthUrls.allowedUrls())
    }

    @Test
    fun `should ignore blank and missing endpoints`() {
        SchemaRegistryOAuthUrls.allowTokenEndpoint(null)
        SchemaRegistryOAuthUrls.allowTokenEndpoint("   ")
        SchemaRegistryOAuthUrls.allowTokenEndpoint(mapOf("bearer.auth.credentials.source" to "STATIC_TOKEN"))

        assertTrue(SchemaRegistryOAuthUrls.allowedUrls().isEmpty())
    }

    @Test
    fun `should read the endpoint from registry properties and java properties`() {
        val registryProperties = mapOf(
            SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL to "https://idp-a.example.com/token"
        )
        val javaProperties = Properties().apply {
            setProperty(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL, "https://idp-b.example.com/token")
        }

        SchemaRegistryOAuthUrls.allowTokenEndpoint(registryProperties)
        SchemaRegistryOAuthUrls.allowTokenEndpoint(javaProperties)

        assertEquals(
            listOf("https://idp-a.example.com/token", "https://idp-b.example.com/token"),
            SchemaRegistryOAuthUrls.allowedUrls()
        )
    }
}
