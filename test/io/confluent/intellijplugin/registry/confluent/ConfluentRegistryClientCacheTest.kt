package io.confluent.intellijplugin.registry.confluent

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.exactly
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

@TestApplication
class ConfluentRegistryClientCacheTest {

    companion object {
        private const val LATEST_VERSION_PATH = "/subjects/.+/versions/latest"
        private const val LATEST_VERSION_RESPONSE =
            """{"subject":"subject","version":1,"id":1,"schemaType":"PROTOBUF","schema":"syntax = \"proto3\";"}"""

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

    @Test
    fun `should not request a schema again while going through a registry with more than 100 subjects`() {
        wireMockServer.stubFor(
            get(urlPathMatching(LATEST_VERSION_PATH))
                .willReturn(aResponse().withStatus(200).withBody(LATEST_VERSION_RESPONSE))
        )
        val client = ConfluentRegistryClient.create("http://localhost:${wireMockServer.port()}", emptyMap())
        val subjects = (1..150).map { "subject-$it-value" }

        repeat(2) {
            subjects.forEach { client.loadSchemaInfo(it) }
        }

        assertEquals(KafkaRegistryFormat.PROTOBUF, client.loadSchemaInfo(subjects.first()).type)
        wireMockServer.verify(exactly(subjects.size), getRequestedFor(urlPathMatching(LATEST_VERSION_PATH)))
    }
}
