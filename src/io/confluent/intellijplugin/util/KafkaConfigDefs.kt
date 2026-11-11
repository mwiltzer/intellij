package io.confluent.intellijplugin.util

import io.confluent.intellijplugin.core.util.withPluginClassLoader
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.ConfigDef

/**
 * The definitions of the configs of the Kafka clients: their names, defaults and documentation.
 *
 * Kafka builds them when a config class is loaded first, and finds the classes that they name, like the default of
 * `sasl.oauthbearer.jwt.retriever.class` since Kafka 4.1, through the context class loader of the thread that loads it.
 * On the threads of the IDE, the UI thread among them, that is not the class loader of the plugin. If the config class
 * is loaded there first, it fails for good: the config class can not be used until the IDE is restarted, and every
 * Kafka client of that kind fails with a `NoClassDefFoundError` from then on.
 *
 * So ask for the definitions here, and do not call `configDef()` of a config class, or anything else that loads one,
 * outside of the block of `withPluginClassLoader`.
 */
object KafkaConfigDefs {

    fun consumer(): ConfigDef = withPluginClassLoader { ConsumerConfig.configDef() }

    fun admin(): ConfigDef = withPluginClassLoader { AdminClientConfig.configDef() }
}
