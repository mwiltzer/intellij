package io.confluent.intellijplugin.common.models

import com.intellij.openapi.util.NlsSafe
import io.confluent.intellijplugin.registry.KafkaRegistryFormat

/**
 * A schema the producer and consumer editors offer.
 *
 * The editors list the names of the schemas first and find out the formats afterwards, because a format costs
 * a registry request. So [schemaFormat] is `null` until it is known, and it is filled in later, in place.
 * That is also why two items are the same schema when their names are, whatever they know about the format yet:
 * a combo box must find the selected schema again when the list is refreshed.
 */
class RegistrySchemaInEditor(
    @NlsSafe val schemaName: String,
    schemaFormat: KafkaRegistryFormat? = null
) : Comparable<RegistrySchemaInEditor> {

    @Volatile
    var schemaFormat: KafkaRegistryFormat? = schemaFormat

    override fun compareTo(other: RegistrySchemaInEditor) = schemaName.compareTo(other.schemaName)

    override fun equals(other: Any?) = other is RegistrySchemaInEditor && other.schemaName == schemaName

    override fun hashCode() = schemaName.hashCode()

    override fun toString() = schemaName
}
