package io.confluent.intellijplugin.registry.confluent

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.exactly
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Base64

/**
 * Exercises the Schema Registry client's built-in `OAUTHBEARER` credential provider end-to-end through
 * [ConfluentRegistryClient.create]: client-credentials token request against a mocked identity provider,
 * followed by registry calls carrying the retrieved bearer token and the Confluent Cloud routing headers.
 */
@TestApplication
class ConfluentRegistryClientOAuthTest {

    companion object {
        private const val CLIENT_ID = "test-client-id"
        private const val CLIENT_SECRET = "test-client-secret"
        private const val SCOPE = "schema-registry"
        private const val LOGICAL_CLUSTER = "lsrc-test"
        private const val IDENTITY_POOL_ID = "pool-test"
        private const val TOKEN_PATH = "/oauth/token"
        private const val SUBJECTS_PATH = "/subjects"

        private lateinit var wireMockServer: WireMockServer

        @JvmStatic
        @BeforeAll
        fun setUpAll() {
            wireMockServer = WireMockServer(wireMockConfig().dynamicPort())
            wireMockServer.start()
        }

        @JvmStatic
        @AfterAll
        fun tearDownAll() {
            wireMockServer.stop()
        }
    }

    private fun loadFixture(name: String): String =
        javaClass.getResourceAsStream("/fixtures/registry/oauth/$name")!!.bufferedReader().use { it.readText() }.trim()

    private val tokenResponseJson = loadFixture("token-response.json")
    private val subjectsResponseJson = loadFixture("subjects-response.json")
    private val accessToken: String = ObjectMapper().readTree(tokenResponseJson).get("access_token").asText()

    private val baseUrl: String
        get() = "http://localhost:${wireMockServer.port()}"

    @AfterEach
    fun cleanUp() {
        wireMockServer.resetAll()
    }

    @Nested
    @DisplayName("OAUTHBEARER credentials source")
    inner class OAuthBearer {

        @Test
        fun `should obtain a client-credentials token and call the registry with it`() {
            stubTokenEndpoint()
            stubSubjectsEndpoint()

            val registryClient = createRegistryClient(oauthProperties())

            registryClient.checkConnection()

            wireMockServer.verify(
                exactly(1),
                postRequestedFor(urlPathEqualTo(TOKEN_PATH))
                    .withHeader("Authorization", equalTo(basicAuthHeader()))
                    .withRequestBody(equalTo("grant_type=client_credentials&scope=$SCOPE"))
            )
            wireMockServer.verify(
                exactly(1),
                getRequestedFor(urlPathEqualTo(SUBJECTS_PATH))
                    .withHeader("Authorization", equalTo("Bearer $accessToken"))
                    .withHeader("target-sr-cluster", equalTo(LOGICAL_CLUSTER))
                    .withHeader("Confluent-Identity-Pool-Id", equalTo(IDENTITY_POOL_ID))
            )
        }

        @Test
        fun `should reuse the cached token across registry calls`() {
            stubTokenEndpoint()
            stubSubjectsEndpoint()

            val registryClient = createRegistryClient(oauthProperties())

            registryClient.checkConnection()
            val subjects = registryClient.internalClient.getAllSubjects()

            assertEquals(listOf("orders-value", "users-value"), subjects.toList())
            wireMockServer.verify(exactly(1), postRequestedFor(urlPathEqualTo(TOKEN_PATH)))
            wireMockServer.verify(exactly(2), getRequestedFor(urlPathEqualTo(SUBJECTS_PATH)))
        }

        @Test
        fun `should work when the context class loader cannot see the plugin classes`() {
            // Registry clients are created outside withPluginClassLoader, unlike the admin, producer and consumer clients
            stubTokenEndpoint()
            stubSubjectsEndpoint()
            val thread = Thread.currentThread()
            val original = thread.contextClassLoader
            thread.contextClassLoader = ClassLoader.getPlatformClassLoader()

            try {
                val subjects = createRegistryClient(oauthProperties()).internalClient.getAllSubjects()

                assertEquals(listOf("orders-value", "users-value"), subjects.toList())
            } finally {
                thread.contextClassLoader = original
            }
        }

        @Test
        fun `should omit routing headers when logical cluster and identity pool are not configured`() {
            stubTokenEndpoint()
            wireMockServer.stubFor(
                get(urlPathEqualTo(SUBJECTS_PATH))
                    .withHeader("Authorization", equalTo("Bearer $accessToken"))
                    .willReturn(jsonResponse(subjectsResponseJson))
            )

            val properties = oauthProperties() -
                    SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER -
                    SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID
            val registryClient = createRegistryClient(properties)

            registryClient.checkConnection()

            val request = wireMockServer.findAll(getRequestedFor(urlPathEqualTo(SUBJECTS_PATH))).single()
            assertTrue(!request.containsHeader("target-sr-cluster"), "target-sr-cluster header must be absent")
            assertTrue(!request.containsHeader("Confluent-Identity-Pool-Id"), "identity pool header must be absent")
        }

        @Test
        fun `should fail when the token endpoint rejects the client credentials`() {
            wireMockServer.stubFor(
                post(urlPathEqualTo(TOKEN_PATH))
                    .willReturn(
                        aResponse()
                            .withStatus(401)
                            .withHeader("Content-Type", "application/json")
                            .withBody("""{"error":"invalid_client"}""")
                    )
            )
            stubSubjectsEndpoint()

            val registryClient = createRegistryClient(oauthProperties())

            val thrown = assertThrows(Throwable::class.java) { registryClient.checkConnection() }

            val tokenEndpointError = generateSequence(thrown) { it.cause }.filterIsInstance<IOException>().firstOrNull()
            assertTrue(
                tokenEndpointError?.message?.contains("401") == true,
                "Expected the token endpoint 401 to surface in the cause chain but got: $thrown"
            )
            wireMockServer.verify(exactly(0), getRequestedFor(urlPathEqualTo(SUBJECTS_PATH)))
        }

        @Test
        fun `should reject configuration without the required client credentials`() {
            val properties = oauthProperties() -
                    SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID -
                    SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET

            assertThrows(ConfigException::class.java) { createRegistryClient(properties) }
        }

        @Test
        fun `should allow-list the token endpoint for the kafka-clients OAuth URL restriction`() {
            createRegistryClient(oauthProperties())

            assertTrue(
                "$baseUrl$TOKEN_PATH" in SchemaRegistryOAuthUrls.allowedUrls(),
                "Token endpoint must be registered in ${SchemaRegistryOAuthUrls.ALLOWED_URLS_PROPERTY}"
            )
        }
    }

    private fun oauthProperties(): Map<String, String> = mapOf(
        SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE to "OAUTHBEARER",
        SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL to "$baseUrl$TOKEN_PATH",
        SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID to CLIENT_ID,
        SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET to CLIENT_SECRET,
        SchemaRegistryClientConfig.BEARER_AUTH_SCOPE to SCOPE,
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER to LOGICAL_CLUSTER,
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID to IDENTITY_POOL_ID,
    )

    private fun createRegistryClient(properties: Map<String, String>): ConfluentRegistryClient =
        ConfluentRegistryClient.create(baseUrl, properties)

    private fun stubTokenEndpoint() {
        wireMockServer.stubFor(
            post(urlPathEqualTo(TOKEN_PATH))
                .withHeader("Authorization", equalTo(basicAuthHeader()))
                .willReturn(jsonResponse(tokenResponseJson))
        )
    }

    private fun stubSubjectsEndpoint() {
        wireMockServer.stubFor(
            get(urlPathEqualTo(SUBJECTS_PATH))
                .withHeader("Authorization", equalTo("Bearer $accessToken"))
                .withHeader("target-sr-cluster", equalTo(LOGICAL_CLUSTER))
                .withHeader("Confluent-Identity-Pool-Id", equalTo(IDENTITY_POOL_ID))
                .willReturn(jsonResponse(subjectsResponseJson))
        )
    }

    private fun jsonResponse(body: String) =
        aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)

    private fun basicAuthHeader(): String =
        "Basic " + Base64.getEncoder().encodeToString("$CLIENT_ID:$CLIENT_SECRET".toByteArray())
}
