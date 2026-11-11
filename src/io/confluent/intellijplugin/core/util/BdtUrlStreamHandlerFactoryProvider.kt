package io.confluent.intellijplugin.core.util

import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.extensions.ExtensionPointName
import java.net.URLStreamHandlerFactory

interface BdtUrlStreamHandlerFactoryProvider {
    fun getProtocol(): String
    fun getStreamHandler(): URLStreamHandlerFactory

    companion object {
        private val logger = Logger.getInstance(this::class.java)
        private val EP_NAME = ExtensionPointName.create<BdtUrlStreamHandlerFactoryProvider>(
            "com.intellij.bigdatatools.rfs.bdtUrlStreamHandlerFactoryProvider"
        )

        fun getForProtocol(protocol: String): BdtUrlStreamHandlerFactoryProvider? = try {
            // The extension point is the one of the Big Data Tools plugin: there is none without it, and with it the
            // providers are of its own interface. They are skipped, not cast, which would fail with a stack trace
            // in idea.log on every connection check.
            val installed = ApplicationManager.getApplication()?.extensionArea?.hasExtensionPoint(EP_NAME) == true
            if (installed) providerFor(protocol, EP_NAME.extensionList) else null
        } catch (t: Throwable) {
            logger.info(t)
            null
        }

        /** The provider of the [protocol] among the [candidates]; the ones that are of another interface are skipped. */
        internal fun providerFor(protocol: String, candidates: List<Any?>): BdtUrlStreamHandlerFactoryProvider? =
            candidates.filterIsInstance<BdtUrlStreamHandlerFactoryProvider>().find {
                it.getProtocol().equals(protocol, ignoreCase = true)
            }
    }
}