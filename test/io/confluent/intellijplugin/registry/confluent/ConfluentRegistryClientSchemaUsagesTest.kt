package io.confluent.intellijplugin.registry.confluent

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.registry.SchemaUsage
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@TestApplication
class ConfluentRegistryClientSchemaUsagesTest {

    companion object {
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

    @AfterEach
    fun cleanUp() {
        wireMockServer.resetAll()
    }

    private fun client() = ConfluentRegistryClient.create("http://localhost:${wireMockServer.port()}", emptyMap())

    private fun json(status: Int, body: String) =
        aResponse().withStatus(status).withHeader("Content-Type", "application/json").withBody(body)

    @Test
    fun `should list the subjects and versions that use a schema`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/schemas/ids/7/versions")).willReturn(
                json(200, """[{"subject":"orders-value","version":3},{"subject":"audit-value","version":1}]""")
            )
        )

        assertEquals(
            listOf(SchemaUsage("orders-value", 3), SchemaUsage("audit-value", 1)),
            client().listSchemaUsages(7)
        )
    }

    @Test
    fun `should list nothing for a schema that no subject uses`() {
        wireMockServer.stubFor(get(urlPathEqualTo("/schemas/ids/7/versions")).willReturn(json(200, "[]")))

        assertEquals(emptyList<SchemaUsage>(), client().listSchemaUsages(7))
    }

    @Test
    fun `should list nothing for a schema ID the registry does not know`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/schemas/ids/7/versions"))
                .willReturn(json(404, """{"error_code":40403,"message":"Schema 7 not found"}"""))
        )

        assertEquals(emptyList<SchemaUsage>(), client().listSchemaUsages(7))
    }

    @Test
    fun `should fail for credentials that are refused`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/schemas/ids/7/versions"))
                .willReturn(json(401, """{"error_code":40101,"message":"Unauthorized"}"""))
        )

        val failure = assertThrows<RestClientException> { client().listSchemaUsages(7) }

        assertEquals(401, failure.status)
    }

    @Test
    fun `should fail for a client that may not read the schema`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/schemas/ids/7/versions"))
                .willReturn(json(403, """{"error_code":40301,"message":"Forbidden"}"""))
        )

        val failure = assertThrows<RestClientException> { client().listSchemaUsages(7) }

        assertEquals(403, failure.status)
    }
}
