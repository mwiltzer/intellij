package io.confluent.intellijplugin.core.monitoring.data.model

import com.intellij.openapi.progress.ProcessCanceledException
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

@TestApplication
class ObjectDataModelAdditionalUpdateTest {

    private val basic = listOf(KafkaSchemaInfo("a"), KafkaSchemaInfo("b"))
    private val detailed = basic.map { it.copy(version = 3) }

    private fun modelOf(
        additionalInfoLoading: suspend (ObjectDataModel<KafkaSchemaInfo>) -> Pair<List<KafkaSchemaInfo>, Throwable?>
    ) = ObjectDataModel(
        KafkaSchemaInfo::name,
        additionalInfoLoading = additionalInfoLoading
    ) { basic to false }.also { it.update() }

    private fun ObjectDataModel<KafkaSchemaInfo>.runAdditionalUpdate() = runBlocking {
        withTimeout(10_000) { launchAdditionalUpdate(this)?.join() }
    }

    @Test
    fun `should replace the basic data by the loaded details`() {
        val model = modelOf { detailed to null }

        model.runAdditionalUpdate()

        assertEquals(detailed, model.data)
        assertNull(model.error)
        assertNull(model.additionalLoadError)
    }

    @Test
    fun `should remember the error the loader reports next to the data`() {
        val failure = IllegalStateException("could not load the details")
        val model = modelOf { basic to failure }

        model.runAdditionalUpdate()

        assertEquals(basic, model.data)
        assertNull(model.error)
        assertSame(failure, model.additionalLoadError)
    }

    @Test
    fun `should not load details when the model failed already`() {
        var loads = 0
        val model = ObjectDataModel(
            KafkaSchemaInfo::name,
            additionalInfoLoading = { loads++; detailed to null }
        ) { throw IllegalStateException("not available") }.also { it.update() }

        model.runAdditionalUpdate()

        assertEquals(0, loads)
        assertNotNull(model.error)
    }

    @Test
    fun `should make a failing loader the error of the model`() {
        val model = modelOf { throw IllegalStateException("boom") }

        model.runAdditionalUpdate()

        assertNotNull(model.error)
        assertNull(model.data)
    }

    @Test
    fun `should keep the data when the loader is cancelled`() {
        val model = modelOf { throw CancellationException("The refresh was cancelled") }

        model.runAdditionalUpdate()

        assertEquals(basic, model.data)
        assertNull(model.error)
        assertNull(model.additionalLoadError)
    }

    @Test
    fun `should keep the data when a blocking call of the loader is cancelled`() {
        val model = modelOf { throw ProcessCanceledException() }

        // The cancelled blocking call must not fail the refresh it belongs to, so this has to return normally.
        model.runAdditionalUpdate()

        assertEquals(basic, model.data)
        assertNull(model.error)
        assertNull(model.additionalLoadError)
    }

    @Test
    fun `should keep the data when the refresh is cancelled while the loader is busy`() = runBlocking<Unit> {
        val started = CompletableDeferred<Unit>()
        val model = modelOf {
            started.complete(Unit)
            CompletableDeferred<Pair<List<KafkaSchemaInfo>, Throwable?>>().await()
        }
        val refresh = CoroutineScope(Job() + Dispatchers.Default)

        val job = model.launchAdditionalUpdate(refresh)!!
        withTimeout(10_000) { started.await() }
        refresh.cancel()
        withTimeout(10_000) { job.join() }

        assertEquals(basic, model.data)
        assertNull(model.error)
        assertNull(model.additionalLoadError)
    }
}
