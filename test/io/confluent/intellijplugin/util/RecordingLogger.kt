package io.confluent.intellijplugin.util

import com.intellij.openapi.diagnostic.Logger
import java.util.concurrent.CopyOnWriteArrayList

/** A logger that keeps what it is told, as `level: message`, for tests that look at what the plugin writes to idea.log. */
internal class RecordingLogger : Logger() {
    val lines = CopyOnWriteArrayList<String>()

    override fun isDebugEnabled() = true
    override fun debug(message: String?, t: Throwable?) {
        lines += "debug: $message"
    }

    override fun info(message: String?, t: Throwable?) {
        lines += "info: $message"
    }

    override fun warn(message: String?, t: Throwable?) {
        lines += "warn: $message"
    }

    override fun error(message: String?, t: Throwable?, vararg details: String) {
        lines += "error: $message"
    }
}
