package io.confluent.intellijplugin.registry.confluent

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.absent
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.exactly
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.registry.common.hasLoadedDetails
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@TestApplication
class ConfluentRegistryClientListSchemasByPrefixTest {

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

    private val client = ConfluentRegistryClient.create("http://localhost:${wireMockServer.port()}", emptyMap())

    private fun json(status: Int, body: String) =
        aResponse().withStatus(status).withHeader("Content-Type", "application/json").withBody(body)

    private fun subjects() = getRequestedFor(urlPathEqualTo("/subjects"))

    private fun stubSubjects(body: String) =
        wireMockServer.stubFor(get(urlPathEqualTo("/subjects")).willReturn(json(200, body)))

    @Test
    fun `should ask the registry for the subjects with the prefix`() {
        stubSubjects("""["orders-key", "orders-value"]""")

        client.listSchemasByPrefix("orders")

        wireMockServer.verify(exactly(1), subjects().withQueryParam("subjectPrefix", equalTo("orders")))
    }

    @Test
    fun `should leave the deleted subjects out unless told to`() {
        stubSubjects("[]")

        client.listSchemasByPrefix("orders")
        wireMockServer.verify(exactly(1), subjects().withQueryParam("deleted", equalTo("false")))

        client.listSchemasByPrefix("orders", registryShowDeletedSubjects = true)
        wireMockServer.verify(exactly(1), subjects().withQueryParam("deleted", equalTo("true")))
    }

    @Test
    fun `should send a prefix with special characters as it is`() {
        stubSubjects("[]")

        client.listSchemasByPrefix("a b&c=d/é:.ctx:")

        wireMockServer.verify(exactly(1), subjects().withQueryParam("subjectPrefix", equalTo("a b&c=d/é:.ctx:")))
    }

    @Test
    fun `should not page the subjects`() {
        stubSubjects("[]")

        client.listSchemasByPrefix("orders")

        wireMockServer.verify(exactly(1), subjects().withQueryParam("limit", absent()))
        wireMockServer.verify(exactly(1), subjects().withQueryParam("offset", absent()))
    }

    @Test
    fun `should list the subjects in the order of their names regardless of the case, but not the folders`() {
        stubSubjects("""["orders-value", "orders-Zebra", "orders(v1)-key", "orders/", "orders-apple", "orders.v2-value"]""")

        val names = client.listSchemasByPrefix("orders")!!.map { it.name }

        assertEquals(listOf("orders(v1)-key", "orders-apple", "orders-value", "orders-Zebra", "orders.v2-value"), names)
    }

    @Test
    fun `should list names only, the details come later`() {
        stubSubjects("""["orders-key", "orders-value"]""")

        assertTrue(client.listSchemasByPrefix("orders")!!.none { it.hasLoadedDetails() })
    }

    @Test
    fun `should list nothing for a prefix that no subject has`() {
        stubSubjects("[]")

        assertEquals(emptyList<String>(), client.listSchemasByPrefix("nothing")!!.map { it.name })
    }

    @Test
    fun `should give nothing for a registry that does not know the parameter`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).willReturn(json(400, """{"error_code":40001,"message":"Unknown parameter"}"""))
        )

        assertNull(client.listSchemasByPrefix("orders"))
    }

    @Test
    fun `should give nothing for a registry that finds the parameter unprocessable`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).willReturn(json(422, """{"error_code":42201,"message":"Invalid"}"""))
        )

        assertNull(client.listSchemasByPrefix("orders"))
    }

    @Test
    fun `should give nothing for a registry that ignores the parameter and lists all its subjects`() {
        stubSubjects("""["orders-key", "orders-value", "payments-key", "payments-value"]""")

        assertNull(client.listSchemasByPrefix("orders"))
    }

    @Test
    fun `should give nothing when only the last subject does not start with the prefix`() {
        stubSubjects("""["orders-key", "orders-value", "payments-value"]""")

        assertNull(client.listSchemasByPrefix("orders"))
    }

    @Test
    fun `should list the subjects of a prefix that is the whole name of one`() {
        stubSubjects("""["orders"]""")

        assertEquals(listOf("orders"), client.listSchemasByPrefix("orders")!!.map { it.name })
    }

    @Test
    fun `should list the subjects of a context as they are named in the registry`() {
        stubSubjects("""[":.ctx:orders-key", ":.ctx:orders-value"]""")

        assertEquals(
            listOf(":.ctx:orders-key", ":.ctx:orders-value"),
            client.listSchemasByPrefix(":.ctx:orders")!!.map { it.name }
        )
    }

    @Test
    fun `should fail for credentials that are refused`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).willReturn(json(401, """{"error_code":40101,"message":"Unauthorized"}"""))
        )

        val failure = assertThrows<RestClientException> { client.listSchemasByPrefix("orders") }

        assertEquals(401, failure.status)
    }

    @Test
    fun `should fail for a client that may not list the subjects`() {
        wireMockServer.stubFor(
            get(urlPathEqualTo("/subjects")).willReturn(json(403, """{"error_code":40301,"message":"Forbidden"}"""))
        )

        val failure = assertThrows<RestClientException> { client.listSchemasByPrefix("orders") }

        assertEquals(403, failure.status)
    }
}
