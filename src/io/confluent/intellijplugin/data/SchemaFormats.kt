package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.KafkaRegistryFormat

/**
 * What the producer and consumer editors need to know about the schemas of the registry.
 *
 * The editors show the names of all schemas right away and find out the formats afterwards, and only of the schemas
 * that matter, because the format of a schema costs a registry request and a registry can hold thousands of schemas.
 */
interface SchemaFormats {

    /**
     * Names of all schemas the editors can reference. Costs one registry request at most.
     * `null` if it failed, which is something else than a registry without schemas.
     */
    suspend fun getSchemaNamesForEditor(): List<String>?

    /**
     * The names that were listed before, also when they are old; `null` if there are none.
     * Never asks the registry: it is meant for showing something right away while [getSchemaNamesForEditor] runs.
     */
    fun getCachedSchemaNames(): List<String>? = null

    /** Format of the schema if it is known already; never asks the registry. */
    fun getKnownSchemaFormat(name: String): KafkaRegistryFormat?

    /** Format of the schema, asked for right away if it is not known yet; `null` if it can't be told. */
    suspend fun getSchemaFormat(name: String): KafkaRegistryFormat?

    /**
     * Reports the formats of the [names]: at once those that are known, the others as soon as they are found,
     * a few at a time. The ones in [first] are done before the others. Returns when all [names] are done.
     */
    suspend fun resolveSchemaFormats(
        names: Collection<String>,
        first: Collection<String> = emptyList(),
        onResolved: (name: String, format: KafkaRegistryFormat) -> Unit
    )
}
