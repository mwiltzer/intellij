package io.confluent.intellijplugin.registry.confluent

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.registry.common.hasLoadedDetails
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

@TestApplication
class ConfluentRegistryClientListSchemasTest {

    companion object {
        private lateinit var wireMockServer: WireMockServer

        @JvmStatic
        @BeforeAll
        fun setUpAll() {
            wireMockServer = WireMockServer(wireMockConfig().dynamicPort())
            wireMockServer.start()
            wireMockServer.stubFor(
                get(urlPathEqualTo("/subjects")).willReturn(
                    aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(
                        """["payments-value", "Orders-value", "orders(v1)-key", "orders.v2-value", "folder/"]"""
                    )
                )
            )
        }

        @JvmStatic
        @AfterAll
        fun tearDownAll() {
            wireMockServer.stop()
        }
    }

    private val client = ConfluentRegistryClient.create("http://localhost:${wireMockServer.port()}", emptyMap())

    private fun names() = client.listSchemas().map { it.name }

    @Test
    fun `should list all subjects in the order of their names regardless of the case, but not the folders`() {
        assertEquals(listOf("orders(v1)-key", "Orders-value", "orders.v2-value", "payments-value"), names())
    }

    @Test
    fun `should list names only, the details come later`() {
        assertTrue(client.listSchemas().none { it.hasLoadedDetails() })
    }

    @Test
    fun `should not let the settings of the tool window decide what is listed`() {
        val settings = KafkaToolWindowSettings.getInstance()
        val showFavoriteSchema = settings.showFavoriteSchema
        settings.showFavoriteSchema = true
        try {
            assertEquals(4, names().size)
        } finally {
            settings.showFavoriteSchema = showFavoriteSchema
        }
    }

    @Test
    fun `should ask for the deleted subjects only when told to`() {
        wireMockServer.resetRequests()

        client.listSchemas()
        wireMockServer.verify(
            1, getRequestedFor(urlPathEqualTo("/subjects")).withQueryParam("deleted", equalTo("false"))
        )

        client.listSchemas(registryShowDeletedSubjects = true)
        wireMockServer.verify(
            1, getRequestedFor(urlPathEqualTo("/subjects")).withQueryParam("deleted", equalTo("true"))
        )
    }
}
