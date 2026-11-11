package io.confluent.intellijplugin.updater

import com.intellij.notification.Notification
import com.intellij.notification.NotificationType
import com.intellij.testFramework.junit5.TestApplication
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.IdentityHashMap

@TestApplication
class KafkaLoadPluginListenerTest {

    private var created = 0

    private fun notification() = Notification("kafka", "Installed ${++created}", NotificationType.INFORMATION)

    @Test
    fun `should show a notification of its own in every project`() {
        val shown = mutableListOf<Pair<Notification, String>>()

        notifyEachProject(listOf("a", "b", "c"), ::notification) { notification, project -> shown += notification to project }

        assertEquals(listOf("a", "b", "c"), shown.map { it.second })
        val distinct = IdentityHashMap<Notification, Unit>().also { map -> shown.forEach { map[it.first] = Unit } }
        assertEquals(3, distinct.size)
    }

    @Test
    fun `should create a notification only for a project that there is`() {
        notifyEachProject(listOf("a", "b"), ::notification) { _, _ -> }

        assertEquals(2, created)
    }

    @Test
    fun `should show nothing when no project is open`() {
        val shown = mutableListOf<Notification>()

        notifyEachProject(emptyList<String>(), ::notification) { notification, _ -> shown += notification }

        assertTrue(shown.isEmpty())
        assertEquals(0, created)
    }

    @Test
    fun `should create a new notification of the installation every time`() {
        assertNotSame(createInstalledNotification(), createInstalledNotification())
    }

    @Test
    fun `should offer to try the plugin in the notification of the installation`() {
        val notification = createInstalledNotification()

        assertEquals(1, notification.actions.size)
    }
}
