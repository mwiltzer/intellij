package io.confluent.intellijplugin.core.util

import io.confluent.intellijplugin.util.ChildFirstClassLoader
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.net.URL
import java.net.URLClassLoader

class ClassLoaderUtilTest {

    // The context class loader of a thread of the IDE, which does not see the classes of the plugin.
    private val ofTheIde = URLClassLoader(emptyArray<URL>(), ClassLoader.getPlatformClassLoader())
    private lateinit var before: ClassLoader

    @BeforeEach
    fun useTheClassLoaderOfTheIde() {
        before = Thread.currentThread().contextClassLoader
        Thread.currentThread().contextClassLoader = ofTheIde
    }

    @AfterEach
    fun restore() {
        Thread.currentThread().contextClassLoader = before
        ofTheIde.close()
    }

    @Test
    fun `should run the block with the class loader of the plugin as the context class loader`() {
        withPluginClassLoader {
            assertSame(this::class.java.classLoader, Thread.currentThread().contextClassLoader)
            assertNotSame(ofTheIde, Thread.currentThread().contextClassLoader)
        }
    }

    @Test
    fun `should use the class loader of the object it is called on`() {
        val other = ChildFirstClassLoader(Probe::class.java.classLoader) { it == Probe::class.java.name }
        val inOther = other.loadClass(Probe::class.java.name)
        assertNotSame(Probe::class.java, inOther)
        val probe = inOther.getDeclaredConstructor().newInstance()

        probe.withPluginClassLoader {
            assertSame(other, Thread.currentThread().contextClassLoader)
        }
    }

    @Test
    fun `should give back what the block returns`() {
        assertEquals("result", withPluginClassLoader { "result" })
    }

    @Test
    fun `should put the context class loader back after the block`() {
        withPluginClassLoader { }

        assertSame(ofTheIde, Thread.currentThread().contextClassLoader)
    }

    @Test
    fun `should put the context class loader back when the block fails`() {
        assertThrows<IllegalStateException> {
            withPluginClassLoader { error("failed") }
        }

        assertSame(ofTheIde, Thread.currentThread().contextClassLoader)
    }

    class Probe
}
