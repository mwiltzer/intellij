package io.confluent.intellijplugin.util

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.common.settings.StorageConsumerConfig
import io.confluent.intellijplugin.consumer.editor.KafkaConsumerSettings
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.lang.reflect.InvocationTargetException
import java.net.URL
import java.net.URLClassLoader

/**
 * Kafka builds the definitions of its configs when a config class is loaded first, and since Kafka 4.1 that needs the
 * context class loader of the thread to see the classes of Kafka: the default of `sasl.oauthbearer.jwt.retriever.class`
 * is one. The threads of the IDE, the UI thread among them, have a context class loader that does not. If the first
 * load happens there it fails for good, and every Kafka client fails with a `NoClassDefFoundError` after that.
 *
 * Whether a config class was loaded already depends on what ran before in the JVM of the tests, and a failed load
 * would break the tests that run after it. So each test loads Kafka and the classes it tests again, in a world of its
 * own, see [worldOf], and runs on a thread without Kafka in its context class loader, see [withoutKafkaInContext].
 */
@TestApplication
class KafkaConfigDefsTest {

    /**
     * A class loader that loads Kafka, and the classes of the plugin given by [fresh], again. No config class of Kafka
     * is loaded in it. Everything else they use comes from the class loader of the tests.
     */
    private fun worldOf(vararg fresh: Class<*>): ClassLoader {
        val freshNames = fresh.map { it.name }
        return ChildFirstClassLoader(KafkaConfigDefsTest::class.java.classLoader) { name ->
            name.startsWith("org.apache.kafka.") || freshNames.any { name == it || name.startsWith("$it$") }
        }
    }

    /** Runs [block] on this thread with a context class loader that does not see Kafka, like the UI thread of the IDE. */
    private fun <T> withoutKafkaInContext(block: (withoutKafka: ClassLoader) -> T): T {
        val thread = Thread.currentThread()
        val before = thread.contextClassLoader
        return URLClassLoader(emptyArray<URL>(), ClassLoader.getPlatformClassLoader()).use { withoutKafka ->
            thread.contextClassLoader = withoutKafka
            try {
                block(withoutKafka)
            } finally {
                thread.contextClassLoader = before
            }
        }
    }

    private fun <T> unwrapped(call: () -> T): T = try {
        call()
    } catch (e: InvocationTargetException) {
        throw e.targetException
    }

    private fun Class<*>.invokeOnObject(method: String): Any? =
        unwrapped { getMethod(method).invoke(getField("INSTANCE").get(null)) }

    // What the definitions define. They are of the classes of a world, so they are read without their type.
    private fun namesOf(configDef: Any?): Set<Any?> =
        (configDef!!.javaClass.getMethod("configKeys").invoke(configDef) as Map<*, *>).keys

    @Nested
    inner class `the reason for it` {

        @Test
        fun `should show that Kafka fails for good to load a config class on a thread without Kafka in its context class loader`() {
            val world = worldOf()

            withoutKafkaInContext {
                val first = assertThrows<ExceptionInInitializerError> {
                    Class.forName(ConsumerConfig::class.java.name, true, world)
                }
                // If Kafka does not need the context class loader any more, KafkaConfigDefs is not needed either.
                assertEquals("org.apache.kafka.common.config.ConfigException", first.cause?.javaClass?.name)
                assertTrue(first.cause?.message.orEmpty().contains("could not be found"), first.cause?.message)

                val second = assertThrows<NoClassDefFoundError> {
                    Class.forName(ConsumerConfig::class.java.name, true, world)
                }
                assertTrue(second.message.orEmpty().contains("Could not initialize class"), second.message)
            }
        }

        @Test
        fun `should show that Kafka loads its config classes when the context class loader sees Kafka`() {
            val world = worldOf()
            val thread = Thread.currentThread()
            val before = thread.contextClassLoader
            thread.contextClassLoader = world
            try {
                Class.forName(ConsumerConfig::class.java.name, true, world)
                Class.forName(AdminClientConfig::class.java.name, true, world)
            } finally {
                thread.contextClassLoader = before
            }
        }
    }

    @Nested
    inner class `the definitions of the configs` {

        @Test
        fun `should be given for a consumer on a thread without Kafka in its context class loader`() {
            val defs = worldOf(KafkaConfigDefs::class.java).loadClass(KafkaConfigDefs::class.java.name)

            withoutKafkaInContext {
                val names = namesOf(defs.invokeOnObject("consumer"))

                assertTrue(KafkaConsumerSettings.ALL_PROPERTIES.all { it in names }, "$names")
                // The first load must not have spoiled the class for the next one.
                assertEquals(names, namesOf(defs.invokeOnObject("consumer")))
            }
        }

        @Test
        fun `should be given for an admin client on a thread without Kafka in its context class loader`() {
            val defs = worldOf(KafkaConfigDefs::class.java).loadClass(KafkaConfigDefs::class.java.name)

            withoutKafkaInContext {
                val names = namesOf(defs.invokeOnObject("admin"))

                assertTrue("bootstrap.servers" in names, "$names")
                assertEquals(names, namesOf(defs.invokeOnObject("admin")))
            }
        }

        @Test
        fun `should be given for both when the consumer was asked for first`() {
            val defs = worldOf(KafkaConfigDefs::class.java).loadClass(KafkaConfigDefs::class.java.name)

            withoutKafkaInContext {
                defs.invokeOnObject("consumer")

                assertTrue("bootstrap.servers" in namesOf(defs.invokeOnObject("admin")))
            }
        }

        @Test
        fun `should leave the context class loader of the thread as it was`() {
            val defs = worldOf(KafkaConfigDefs::class.java).loadClass(KafkaConfigDefs::class.java.name)

            withoutKafkaInContext { withoutKafka ->
                defs.invokeOnObject("consumer")
                assertSame(withoutKafka, Thread.currentThread().contextClassLoader)

                defs.invokeOnObject("admin")
                assertSame(withoutKafka, Thread.currentThread().contextClassLoader)
            }
        }
    }

    @Nested
    inner class `the places that load a config class` {

        @Test
        fun `should create the advanced settings of a consumer on a thread without Kafka in its context class loader`() {
            val world = worldOf(KafkaConfigDefs::class.java, KafkaConsumerSettings::class.java)
            val settingsClass = world.loadClass(KafkaConsumerSettings::class.java.name)

            withoutKafkaInContext {
                val settings = unwrapped {
                    settingsClass.getConstructor(Set::class.java).newInstance(KafkaConsumerSettings.ALL_PROPERTIES)
                }

                // Nothing was changed, so nothing is different from the defaults of Kafka.
                val properties = unwrapped { settingsClass.getMethod("getProperties").invoke(settings) } as Map<*, *>
                assertTrue(properties.isEmpty(), "$properties")
            }
        }

        @Test
        fun `should apply and read the settings of a consumer on a thread without Kafka in its context class loader`() {
            val world = worldOf(KafkaConfigDefs::class.java, KafkaConsumerSettings::class.java)
            val settingsClass = world.loadClass(KafkaConsumerSettings::class.java.name)
            val config = StorageConsumerConfig(properties = KafkaConsumerSettings.ALL_PROPERTIES.associateWith { "1" })

            withoutKafkaInContext {
                val settings = unwrapped {
                    settingsClass.getConstructor(Set::class.java).newInstance(KafkaConsumerSettings.ALL_PROPERTIES)
                }
                unwrapped { settingsClass.getMethod("applyConfig", StorageConsumerConfig::class.java).invoke(settings, config) }

                val properties = unwrapped { settingsClass.getMethod("getProperties").invoke(settings) } as Map<*, *>
                assertEquals(KafkaConsumerSettings.ALL_PROPERTIES, properties.keys)
            }
        }

        @Test
        fun `should describe the properties of an admin client on a thread without Kafka in its context class loader`() {
            val world = worldOf(KafkaConfigDefs::class.java, KafkaPropertiesUtils::class.java)
            val utils = world.loadClass(KafkaPropertiesUtils::class.java.name)

            withoutKafkaInContext {
                val descriptions = utils.invokeOnObject("getAdminPropertiesDescriptions") as List<*>

                assertFalse(descriptions.isEmpty())
            }
        }
    }
}
