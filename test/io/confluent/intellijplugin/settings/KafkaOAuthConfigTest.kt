package io.confluent.intellijplugin.settings

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class KafkaOAuthConfigTest {

    private val fullConfig = KafkaOAuthConfig(
        tokenEndpointUrl = "https://login.example.com/oauth/token",
        clientId = "client-id",
        clientSecret = "client-secret",
        scope = "schema-registry",
        logicalCluster = "lsrc-abc123",
        identityPoolId = "pool-xyz789",
    )

    private val fullProperties = mapOf(
        SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE to "OAUTHBEARER",
        SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL to "https://login.example.com/oauth/token",
        SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID to "client-id",
        SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET to "client-secret",
        SchemaRegistryClientConfig.BEARER_AUTH_SCOPE to "schema-registry",
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER to "lsrc-abc123",
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID to "pool-xyz789",
    )

    @Nested
    @DisplayName("toRegistryProperties")
    inner class ToRegistryProperties {

        @Test
        fun `should emit the OAUTHBEARER credentials source and all configured keys`() {
            assertEquals(fullProperties, fullConfig.toRegistryProperties())
        }

        @Test
        fun `should map blank optional values to null so the keys get removed`() {
            val properties = KafkaOAuthConfig(
                tokenEndpointUrl = "https://login.example.com/oauth/token",
                clientId = "client-id",
                clientSecret = "client-secret",
                scope = "   ",
            ).toRegistryProperties()

            assertEquals("OAUTHBEARER", properties[SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE])
            assertEquals("client-id", properties[SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID])
            assertTrue(properties.containsKey(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE))
            assertNull(properties[SchemaRegistryClientConfig.BEARER_AUTH_SCOPE])
            assertNull(properties[SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER])
            assertNull(properties[SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID])
        }

        @Test
        fun `should always cover every managed key`() {
            val properties = KafkaOAuthConfig().toRegistryProperties()

            assertTrue(properties.keys.containsAll(KafkaOAuthConfig.MANAGED_KEYS))
            KafkaOAuthConfig.MANAGED_KEYS.forEach { assertNull(properties[it], "Expected $it to be cleared") }
            assertEquals("OAUTHBEARER", properties[SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE])
        }

        @Test
        fun `should trim surrounding whitespace but keep the secret verbatim`() {
            val properties = KafkaOAuthConfig(
                tokenEndpointUrl = " https://login.example.com/oauth/token ",
                clientId = " client-id ",
                clientSecret = " secret with spaces ",
            ).toRegistryProperties()

            assertEquals("https://login.example.com/oauth/token", properties[SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL])
            assertEquals("client-id", properties[SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID])
            assertEquals(" secret with spaces ", properties[SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET])
        }
    }

    @Nested
    @DisplayName("fromRegistryProperties")
    inner class FromRegistryProperties {

        @Test
        fun `should round-trip a full configuration`() {
            assertEquals(fullConfig, KafkaOAuthConfig.fromRegistryProperties(fullProperties))
        }

        @Test
        fun `should tolerate missing optional keys`() {
            val properties = fullProperties -
                    SchemaRegistryClientConfig.BEARER_AUTH_SCOPE -
                    SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER -
                    SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID

            val config = KafkaOAuthConfig.fromRegistryProperties(properties)

            assertEquals(fullConfig.copy(scope = "", logicalCluster = "", identityPoolId = ""), config)
        }

        @Test
        fun `should return an empty configuration for unrelated properties`() {
            val config = KafkaOAuthConfig.fromRegistryProperties(mapOf("schema.registry.url" to "http://localhost:8081"))

            assertEquals(KafkaOAuthConfig(), config)
        }
    }

    @Nested
    @DisplayName("isOAuth")
    inner class IsOAuth {

        @Test
        fun `should detect the OAUTHBEARER credentials source`() {
            assertTrue(KafkaOAuthConfig.isOAuth(fullProperties))
        }

        @Test
        fun `should not treat a static bearer token as OAuth`() {
            val properties = mapOf(
                SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE to "STATIC_TOKEN",
                SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG to "token",
            )

            assertFalse(KafkaOAuthConfig.isOAuth(properties))
        }

        @Test
        fun `should be false without a credentials source`() {
            assertFalse(KafkaOAuthConfig.isOAuth(emptyMap()))
            assertFalse(KafkaOAuthConfig.isOAuth(fullProperties - SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE))
        }
    }
}
