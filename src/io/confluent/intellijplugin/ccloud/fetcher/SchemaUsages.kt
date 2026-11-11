package io.confluent.intellijplugin.ccloud.fetcher

import io.confluent.intellijplugin.ccloud.client.CCloudApiException
import io.confluent.intellijplugin.registry.SchemaUsage
import io.confluent.intellijplugin.util.catchFailures
import java.net.HttpURLConnection

/**
 * The subjects, and the versions of them, that the schema with the ID [schemaId] is registered as. Nothing for an ID
 * the registry does not know; `null` if it could not be asked, which [onFailure] is told about.
 */
internal suspend fun DataPlaneFetcher.listSchemaUsages(
    schemaId: Int,
    onFailure: (Throwable) -> Unit = {}
): List<SchemaUsage>? = catchFailures<List<SchemaUsage>?>(onFailure = { failure ->
    if ((failure as? CCloudApiException)?.statusCode == HttpURLConnection.HTTP_NOT_FOUND) {
        emptyList()
    } else {
        onFailure(failure)
        null
    }
}) {
    getSchemaVersionsById(schemaId).map { SchemaUsage(it.subject, it.version) }
}
