package io.confluent.intellijplugin.core.util

import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import java.net.URLStreamHandlerFactory

class BdtUrlStreamHandlerFactoryProviderTest {

    private class Provider(private val protocol: String) : BdtUrlStreamHandlerFactoryProvider {
        override fun getProtocol() = protocol
        override fun getStreamHandler() = URLStreamHandlerFactory { null }
    }

    // What the extension point of Big Data Tools has: providers of the interface of that plugin, not of this one.
    private class ProviderOfAnotherPlugin

    private val hdfs = Provider("hdfs")
    private val s3 = Provider("s3")

    @Test
    fun `should find the provider of the protocol`() {
        assertSame(s3, BdtUrlStreamHandlerFactoryProvider.providerFor("s3", listOf(hdfs, s3)))
    }

    @Test
    fun `should find it regardless of the case of the protocol`() {
        assertSame(hdfs, BdtUrlStreamHandlerFactoryProvider.providerFor("HDFS", listOf(hdfs, s3)))
    }

    @Test
    fun `should find no provider for another protocol`() {
        assertNull(BdtUrlStreamHandlerFactoryProvider.providerFor("ftp", listOf(hdfs, s3)))
    }

    @Test
    fun `should find no provider among none`() {
        assertNull(BdtUrlStreamHandlerFactoryProvider.providerFor("s3", emptyList()))
    }

    @Test
    fun `should skip providers of another plugin instead of failing on them`() {
        val found = BdtUrlStreamHandlerFactoryProvider.providerFor("s3", listOf(ProviderOfAnotherPlugin(), s3))

        assertSame(s3, found)
    }

    @Test
    fun `should find no provider when there are only those of another plugin`() {
        assertNull(BdtUrlStreamHandlerFactoryProvider.providerFor("s3", listOf(ProviderOfAnotherPlugin(), null)))
    }

    @Test
    fun `should find no provider when the extension point is not there`() {
        // The extension point belongs to the Big Data Tools plugin, which the tests do not have.
        assertNull(BdtUrlStreamHandlerFactoryProvider.getForProtocol("s3"))
    }
}
