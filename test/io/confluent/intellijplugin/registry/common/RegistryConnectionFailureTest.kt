package io.confluent.intellijplugin.registry.common

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.registry.confluent.ConfluentRegistryClient
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.exceptions.SchemaRegistryOauthTokenRetrieverException
import org.apache.kafka.common.KafkaException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.ConnectException
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.CertPathBuilderException
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

@TestApplication
class RegistryConnectionFailureTest {

    private val tlsHint get() = KafkaMessagesBundle.message("connection.kafka.registry.hint.tls")
    private val oauthHint get() = KafkaMessagesBundle.message("connection.kafka.registry.hint.oauth")

    private fun untrustedCertificate() = SSLHandshakeException("(certificate_unknown) PKIX path building failed").apply {
        initCause(CertPathBuilderException("unable to find valid certification path to requested target"))
    }

    private fun rejectedTokenRequest() = SchemaRegistryOauthTokenRetrieverException(
        "Error while fetching Oauth Token for Schema Registry", RuntimeException("token endpoint said no")
    )

    @Nested
    @DisplayName("describe")
    inner class Describe {

        @Test
        fun `should say why the registry is not available`() {
            val text = RegistryConnectionFailure.describe(ConnectException("Connection refused"))

            assertEquals(
                KafkaMessagesBundle.message(
                    "connection.kafka.registry.is.not.available.reason", "ConnectException: Connection refused"
                ),
                text
            )
        }

        @Test
        fun `should name the exception when its message alone would not say much`() {
            val text = RegistryConnectionFailure.describe(java.net.UnknownHostException("registry.example.com"))

            assertTrue(text.contains("UnknownHostException: registry.example.com"), text)
        }

        @Test
        fun `should append the TLS hint after the reason`() {
            val text = RegistryConnectionFailure.describe(untrustedCertificate())

            assertTrue(text.contains("SSLHandshakeException: (certificate_unknown) PKIX path building failed"), text)
            assertTrue(text.endsWith(tlsHint), text)
        }

        @Test
        fun `should not add a hint when nothing is known about the cure`() {
            val text = RegistryConnectionFailure.describe(ConnectException("Connection refused"))

            assertFalse(text.contains(tlsHint), text)
            assertFalse(text.contains(oauthHint), text)
        }
    }

    @Nested
    @DisplayName("hintFor")
    inner class HintFor {

        @Test
        fun `should hint at the truststore when the certificate path cannot be built`() {
            assertEquals(tlsHint, RegistryConnectionFailure.hintFor(untrustedCertificate()))
        }

        @Test
        fun `should hint at the truststore when the trust failure is wrapped deeper in the chain`() {
            val wrapped = RuntimeException("request failed", RuntimeException("io failure", untrustedCertificate()))

            assertEquals(tlsHint, RegistryConnectionFailure.hintFor(wrapped))
        }

        @Test
        fun `should hint at the truststore for a certificate that does not validate`() {
            val failure = RuntimeException(java.security.cert.CertificateExpiredException("NotAfter: yesterday"))

            assertEquals(tlsHint, RegistryConnectionFailure.hintFor(failure))
        }

        @Test
        fun `should hint at the OAuth settings when the registry token cannot be fetched`() {
            assertEquals(oauthHint, RegistryConnectionFailure.hintFor(rejectedTokenRequest()))
        }

        @Test
        fun `should hint at the OAuth settings when Kafka rejects the token request`() {
            val rejected = KafkaException(
                org.apache.kafka.common.security.oauthbearer.internals.secured.UnretryableException(
                    java.io.IOException("The response code 401 was encountered reading the token endpoint response")
                )
            )

            assertEquals(oauthHint, RegistryConnectionFailure.hintFor(rejected))
        }

        @Test
        fun `should prefer the TLS hint when the token endpoint certificate is not trusted`() {
            val failure = SchemaRegistryOauthTokenRetrieverException(
                "Error while fetching Oauth Token for Schema Registry", untrustedCertificate()
            )

            assertEquals(tlsHint, RegistryConnectionFailure.hintFor(failure))
        }

        @Test
        fun `should give no hint for a plain network failure`() {
            assertNull(RegistryConnectionFailure.hintFor(ConnectException("Connection refused")))
        }

        @Test
        fun `should terminate on a cyclic cause chain`() {
            val first = RuntimeException()
            val second = RuntimeException()
            first.initCause(second)
            second.initCause(first)

            assertNull(RegistryConnectionFailure.hintFor(first))
            assertEquals("RuntimeException", RegistryConnectionFailure.reasonOf(first))
        }
    }

    @Nested
    @DisplayName("reasonOf")
    inner class ReasonOf {

        @Test
        fun `should flatten line breaks so the text stays on one line`() {
            val failure = RuntimeException("Unrecognized token 'not'\n at [Source: REDACTED]\r\n\tline: 1")

            assertEquals(
                "RuntimeException: Unrecognized token 'not' at [Source: REDACTED] line: 1",
                RegistryConnectionFailure.reasonOf(failure)
            )
        }

        @Test
        fun `should cap a very long reason`() {
            val reason = RegistryConnectionFailure.reasonOf(RuntimeException("x".repeat(5_000)))

            assertTrue(reason.length < 400, "length was ${reason.length}")
            assertTrue(reason.endsWith("…"), reason)
        }

        @Test
        fun `should keep a short reason untouched`() {
            assertEquals(
                "ConnectException: Connection refused",
                RegistryConnectionFailure.reasonOf(ConnectException("Connection refused"))
            )
        }
    }

    // These run the real client, so they notice when a library upgrade changes the exceptions the hints rely on.
    @Nested
    @DisplayName("real failures")
    inner class RealFailures {

        private var server: WireMockServer? = null

        @AfterEach
        fun stopServer() {
            server?.stop()
        }

        private fun start(httpsOnly: Boolean = false): WireMockServer {
            val config = wireMockConfig().apply {
                if (httpsOnly) httpDisabled(true).dynamicHttpsPort() else dynamicPort()
            }
            return WireMockServer(config).also {
                it.start()
                server = it
            }
        }

        private fun failureOf(url: String, properties: Map<String, String> = emptyMap()): Throwable =
            requireNotNull(
                runCatching { ConfluentRegistryClient.create(url, properties).checkConnection() }.exceptionOrNull()
            ) { "the registry check was expected to fail" }

        private fun oauthProperties(tokenEndpoint: String) = mapOf(
            SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE to "OAUTHBEARER",
            SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL to tokenEndpoint,
            SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID to "client-id",
            SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET to "client-secret",
            SchemaRegistryClientConfig.BEARER_AUTH_SCOPE to "schema-registry",
        )

        // A truststore that holds one unrelated root CA: the server certificate cannot chain to it, which is
        // what a user gets after pointing the registry settings at a truststore that lacks the registry's CA.
        private fun truststoreWithoutTheServerCa(directory: Path): Map<String, String> {
            val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            factory.init(null as KeyStore?)
            val unrelatedCa = factory.trustManagers.filterIsInstance<X509TrustManager>()
                .flatMap { it.acceptedIssuers.asList() }
                .firstOrNull()
            assumeTrue(unrelatedCa != null, "the JVM exposes no default trust anchors to build a truststore from")

            val password = "changeit"
            val location = directory.resolve("truststore.p12")
            KeyStore.getInstance("PKCS12").run {
                load(null, null)
                setCertificateEntry("unrelated-ca", unrelatedCa)
                Files.newOutputStream(location).use { store(it, password.toCharArray()) }
            }
            return mapOf(
                "schema.registry.ssl.truststore.location" to location.toString(),
                "schema.registry.ssl.truststore.password" to password,
                "schema.registry.ssl.truststore.type" to "PKCS12",
            )
        }

        @Test
        fun `should explain a registry whose certificate is not in the truststore`(@TempDir directory: Path) {
            val https = start(httpsOnly = true)

            val failure = failureOf("https://localhost:${https.httpsPort()}", truststoreWithoutTheServerCa(directory))

            val text = RegistryConnectionFailure.describe(failure)
            assertTrue(text.contains("SSLHandshakeException"), text)
            assertTrue(text.endsWith(tlsHint), text)
        }

        @Test
        fun `should point at the OAuth settings when the token request is rejected`() {
            val http = start()
            http.stubFor(
                post(urlPathEqualTo("/oauth/token")).willReturn(
                    aResponse().withStatus(401).withBody("""{"error":"invalid_client"}""")
                )
            )

            val failure = failureOf("http://localhost:${http.port()}", oauthProperties("http://localhost:${http.port()}/oauth/token"))

            val text = RegistryConnectionFailure.describe(failure)
            assertTrue(text.contains("401"), text)
            assertTrue(text.endsWith(oauthHint), text)
        }

        @Test
        fun `should show the HTTP error of the registry without a hint`() {
            val http = start()
            http.stubFor(
                get(urlPathEqualTo("/subjects")).willReturn(
                    aResponse().withStatus(404).withBody("""{"error_code":404,"message":"HTTP 404 Not Found"}""")
                )
            )

            val text = RegistryConnectionFailure.describe(failureOf("http://localhost:${http.port()}"))

            assertTrue(text.contains("HTTP 404 Not Found"), text)
            assertFalse(text.contains(tlsHint), text)
            assertFalse(text.contains(oauthHint), text)
        }

        @Test
        fun `should show that nothing listens on the registry port`() {
            val text = RegistryConnectionFailure.describe(failureOf("http://localhost:1"))

            assertTrue(text.contains("ConnectException"), text)
            assertFalse(text.contains(tlsHint), text)
        }
    }
}
