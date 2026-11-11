package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.SchemaUsage

/**
 * What the details of a record learn from the registry about the schema the record was written with: the record
 * tells the ID of the schema, and the registry the subjects, and the versions of them, that the schema is registered as.
 */
interface SchemaUsageSource {

    /**
     * The subjects and versions of the schema with the ID [schemaId]. Nothing if the registry does not know the ID;
     * `null` if the registry could not be asked, or has no such thing as a schema ID.
     */
    suspend fun getSchemaUsages(schemaId: Int): List<SchemaUsage>?

    /** What [getSchemaUsages] told lately, `null` if that is not known. Never asks the registry. */
    fun getCachedSchemaUsages(schemaId: Int): List<SchemaUsage>? = null
}
