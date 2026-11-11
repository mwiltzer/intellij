package io.confluent.intellijplugin.updater

import com.intellij.ide.plugins.DynamicPluginListener
import com.intellij.ide.plugins.IdeaPluginDescriptor
import com.intellij.notification.Notification
import com.intellij.notification.Notification.CollapseActionsDirection
import com.intellij.notification.NotificationAction
import com.intellij.notification.NotificationGroup
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.openapi.actionSystem.CommonDataKeys
import com.intellij.openapi.project.ProjectManager
import com.intellij.openapi.wm.ToolWindowManager
import io.confluent.intellijplugin.core.constants.BdtConnectionType
import io.confluent.intellijplugin.core.constants.BdtPlugins
import io.confluent.intellijplugin.core.settings.ConnectionSettings
import io.confluent.intellijplugin.core.settings.manager.RfsConnectionDataManager
import io.confluent.intellijplugin.core.util.executeOnPooledThread
import io.confluent.intellijplugin.core.util.invokeLater
import io.confluent.intellijplugin.settings.KafkaConnectionGroup
import io.confluent.intellijplugin.telemetry.logUser
import io.confluent.intellijplugin.toolwindow.KafkaMonitoringToolWindowController
import io.confluent.intellijplugin.util.KafkaMessagesBundle

private val defaultNotificationGroup: NotificationGroup
    get() =
        NotificationGroupManager.getInstance().getNotificationGroup("kafka")

/**
 * The notification that tells that the plugin was installed. A new one every time: a notification has one balloon, and
 * showing it in a second project hides the one it has in the first, see `Notification.setBalloon`.
 */
internal fun createInstalledNotification(): Notification {
    val notification = defaultNotificationGroup.createNotification(
        KafkaMessagesBundle.message("kafka.plugin.installed"),
        NotificationType.INFORMATION
    )

    notification.collapseDirection = CollapseActionsDirection.KEEP_LEFTMOST
    notification.addAction(
        NotificationAction.create(KafkaMessagesBundle.message("kafka.plugin.try.it")) { e ->
            val project = e.getData(CommonDataKeys.PROJECT) ?: return@create
            ToolWindowManager.getInstance(project).getToolWindow(KafkaMonitoringToolWindowController.TOOL_WINDOW_ID)
                ?.show()
            executeOnPooledThread {
                val kafkaConnections = RfsConnectionDataManager.instance?.getConnections(project)
                    ?.filter { it.groupId == BdtConnectionType.KAFKA.id } ?: emptyList()
                if (kafkaConnections.isEmpty())
                    invokeLater {
                        ConnectionSettings.create(
                            project,
                            KafkaConnectionGroup(),
                            KafkaConnectionGroup().createBlankData(),
                            applyIfOk = true
                        )
                    }
            }
            notification.expire()
        }
    )
    return notification
}

/** Shows a notification of its own in every one of the [projects]. */
internal fun <P> notifyEachProject(projects: Collection<P>, create: () -> Notification, show: (Notification, P) -> Unit) {
    projects.forEach { project -> show(create(), project) }
}

internal class KafkaLoadPluginListener : DynamicPluginListener {
    override fun pluginLoaded(pluginDescriptor: IdeaPluginDescriptor) {
        if (pluginDescriptor.pluginId.idString != BdtPlugins.KAFKA_ID)
            return

        // Send initial identify when plugin is installed dynamically
        logUser(buildMap {
            put("pluginVersion", pluginDescriptor.version)
            put("pluginInstalled", true)
        })

        notifyEachProject(ProjectManager.getInstance().openProjects.asList(), ::createInstalledNotification) { notification, project ->
            invokeLater {
                if (!project.isDisposed) notification.notify(project)
            }
        }
    }

    override fun beforePluginUnload(pluginDescriptor: IdeaPluginDescriptor, isUpdate: Boolean) {
        if (pluginDescriptor.pluginId.idString != BdtPlugins.KAFKA_ID)
            return

        // Send final identify event before plugin is uninstalled
        logUser(buildMap {
            put("pluginVersion", pluginDescriptor.version)
            put("pluginInstalled", false)
            put("isUpdate", isUpdate)
        })
    }
}