package io.confluent.intellijplugin.registry.confluent

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.absent
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.exactly
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.matching
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.http.Fault
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

@TestApplication
class ConfluentRegistryClientCheckConnectionTest {

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

    private fun subjectRequests() = getRequestedFor(urlPathEqualTo("/subjects"))

    @Test
    fun `should ask for a single subject`() {
        wireMockServer.stubFor(get(urlPathEqualTo("/subjects")).willReturn(json(200, """["a-value"]""")))

        client().checkConnection()

        wireMockServer.verify(
            exactly(1),
            subjectRequests().withQueryParam("limit", equalTo("1")).withQueryParam("offset", equalTo("0"))
        )
        wireMockServer.verify(exactly(0), subjectRequests().withQueryParam("limit", absent()))
    }

    @Test
    fun `should accept a registry without subjects`() {
        wireMockServer.stubFor(get(urlPathEqualTo("/subjects")).willReturn(json(200, "[]")))

        client().checkConnection()
    }

    @Test
    fun `should accept a registry that ignores the page and answers with all subjects`() {
        val everything = (1..500).joinToString(",", "[", "]") { "\"subject-$it-value\"" }
        wireMockServer.stubFor(get(urlPathEqualTo("/subjects")).willReturn(json(200, everything)))

        client().checkConnection()

        wireMockServer.verify(exactly(1), subjectRequests())
    }

    @Test
    fun `should fail for credentials that are refused, without asking again`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).willReturn(json(401, """{"error_code":40101,"message":"Unauthorized"}"""))
        )

        val failure = assertThrows<RestClientException> { client().checkConnection() }

        assertEquals(401, failure.status)
        wireMockServer.verify(exactly(1), subjectRequests())
    }

    @Test
    fun `should fail for a client that may not list the subjects, without asking again`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).willReturn(json(403, """{"error_code":40301,"message":"Forbidden"}"""))
        )

        val failure = assertThrows<RestClientException> { client().checkConnection() }

        assertEquals(403, failure.status)
        wireMockServer.verify(exactly(1), subjectRequests())
    }

    @Test
    fun `should fail for a registry that has an error, without listing all subjects instead`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).willReturn(json(500, """{"error_code":50001,"message":"Error"}"""))
        )

        val failure = assertThrows<RestClientException> { client().checkConnection() }

        assertEquals(500, failure.status)
        // The client of the registry tries a request that failed on the server again by itself.
        wireMockServer.verify(exactly(0), subjectRequests().withQueryParam("limit", absent()))
    }

    @Test
    fun `should fail for a registry that does not answer`() {
        wireMockServer.stubFor(get(urlPathEqualTo("/subjects")).willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)))

        assertThrows<IOException> { client().checkConnection() }
    }

    @Test
    fun `should check with all subjects when the registry refuses the page parameters`() {
        listOf(400, 422).forEach { status ->
            wireMockServer.resetAll()
            wireMockServer.stubFor(
                get(urlPathEqualTo("/subjects")).withQueryParam("limit", matching(".*"))
                    .willReturn(json(status, """{"error_code":$status,"message":"Unknown parameter"}"""))
            )
            wireMockServer.stubFor(
                get(urlPathEqualTo("/subjects")).withQueryParam("limit", absent())
                    .willReturn(json(200, """["a-value","b-value"]"""))
            )

            client().checkConnection()

            wireMockServer.verify(exactly(1), subjectRequests().withQueryParam("limit", absent()))
        }
    }

    @Test
    fun `should still fail when the registry refuses the page parameters and the credentials`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).withQueryParam("limit", matching(".*"))
                .willReturn(json(422, """{"error_code":422,"message":"Unknown parameter"}"""))
        )
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).withQueryParam("limit", absent())
                .willReturn(json(401, """{"error_code":40101,"message":"Unauthorized"}"""))
        )

        val failure = assertThrows<RestClientException> { client().checkConnection() }

        assertEquals(401, failure.status)
    }
}
