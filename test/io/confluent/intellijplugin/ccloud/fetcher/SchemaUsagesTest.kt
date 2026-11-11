package io.confluent.intellijplugin.ccloud.fetcher

import io.confluent.intellijplugin.ccloud.client.CCloudApiException
import io.confluent.intellijplugin.ccloud.model.response.SubjectVersionResponse
import io.confluent.intellijplugin.registry.SchemaUsage
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class SchemaUsagesTest {

    // A failure is thrown by the answer: the registry throws what its signature does not declare as well.
    private fun fetcherFailingWith(failure: Throwable): DataPlaneFetcher = mock<DataPlaneFetcher>().also { fetcher ->
        runBlocking { whenever(fetcher.getSchemaVersionsById(7)).thenAnswer { throw failure } }
    }

    private fun fetcherListing(usages: List<SubjectVersionResponse>): DataPlaneFetcher =
        mock<DataPlaneFetcher>().also { fetcher ->
            runBlocking { whenever(fetcher.getSchemaVersionsById(7)).thenReturn(usages) }
        }

    @Test
    fun `should list the subjects and versions that use the schema`() = runBlocking<Unit> {
        val fetcher = fetcherListing(
            listOf(SubjectVersionResponse("orders-value", 3), SubjectVersionResponse("audit-value", 1))
        )

        assertEquals(
            listOf(SchemaUsage("orders-value", 3), SchemaUsage("audit-value", 1)),
            fetcher.listSchemaUsages(7)
        )
    }

    @Test
    fun `should list nothing for a schema that no subject uses`() = runBlocking<Unit> {
        val fetcher = fetcherListing(emptyList())

        assertEquals(emptyList<SchemaUsage>(), fetcher.listSchemaUsages(7))
    }

    @Test
    fun `should list nothing for a schema ID the registry does not know`() = runBlocking<Unit> {
        val failures = mutableListOf<Throwable>()
        val fetcher = fetcherFailingWith(CCloudApiException("not found (HTTP 404)", 404))

        assertEquals(emptyList<SchemaUsage>(), fetcher.listSchemaUsages(7) { failures += it })

        // Not knowing an ID is no failure to tell about.
        assertEquals(emptyList<Throwable>(), failures)
    }

    @Test
    fun `should give null and tell the failure when the registry refuses`() = runBlocking<Unit> {
        val refusal = CCloudApiException("forbidden (HTTP 403)", 403)
        val failures = mutableListOf<Throwable>()
        val fetcher = fetcherFailingWith(refusal)

        assertNull(fetcher.listSchemaUsages(7) { failures += it })

        assertEquals(listOf<Throwable>(refusal), failures)
    }

    @Test
    fun `should give null for a failure of anything but the registry`() = runBlocking<Unit> {
        val fetcher = fetcherFailingWith(IllegalStateException("no connection"))

        assertNull(fetcher.listSchemaUsages(7))
    }

    @Test
    fun `should not take a cancellation for a failure`() = runBlocking<Unit> {
        val failures = mutableListOf<Throwable>()
        val fetcher = fetcherFailingWith(CancellationException("cancelled"))

        assertThrows<CancellationException> { fetcher.listSchemaUsages(7) { failures += it } }

        assertEquals(emptyList<Throwable>(), failures)
    }
}
