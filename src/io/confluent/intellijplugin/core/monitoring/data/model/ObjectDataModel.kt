package io.confluent.intellijplugin.core.monitoring.data.model

import com.intellij.openapi.progress.ProcessCanceledException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlin.reflect.KProperty1

open class ObjectDataModel<T : RemoteInfo>(
    idFieldName: KProperty1<T, Any?>,
    allowAutoRefresh: Boolean = true,
    val additionalInfoLoading: (suspend (ObjectDataModel<T>) -> Pair<List<T>, Throwable?>)? = null,
    override val updater: (DataModel<*>) -> Pair<List<T>, Boolean>
) : ObjectDataModelBase<T>(
    idFieldName,
    allowAutoRefresh
) {
    suspend fun launchAdditionalUpdate(coroutineScope: CoroutineScope): Job? {
        val loadFun = additionalInfoLoading ?: return null

        return coroutineScope.launch {
            try {
                if (error != null)
                    return@launch
                val res = loadFun(this@ObjectDataModel)
                setData(res.first)
                additionalLoadError = res.second
            } catch (e: CancellationException) {
                throw e
            } catch (_: ProcessCanceledException) {
                // The update was cancelled, and blocking calls report that like this. It is no error of the model, and
                // it must not fail the refresh it belongs to: the data of the first phase stays as it is.
                return@launch
            } catch (t: Throwable) {
                setError(t)
            }
        }
    }
}
