package io.confluent.intellijplugin.settings

import io.confluent.intellijplugin.core.settings.ModificationKey
import io.confluent.intellijplugin.core.settings.fields.WrappedComponent
import io.confluent.intellijplugin.rfs.KafkaConnectionData
import org.jetbrains.annotations.Nls
import javax.swing.JComponent

/**
 * Brings a component into the validation of the settings form that runs before a connection is tested,
 * although it is not bound to a connection setting itself (the OAuth fields, for one, end up in the registry
 * properties). It takes part in nothing else: the component is laid out elsewhere, and there is nothing to
 * apply or to save.
 *
 * The [label] names the component in the errors, the [component] is looked up when it is needed
 * because the form creates it only after the fields have been listed.
 */
internal class ComponentValidationField(
    @Nls label: String,
    private val component: () -> JComponent
) : WrappedComponent<KafkaConnectionData>(ModificationKey(label)) {

    override fun getValue(): Any? = null

    override fun getComponent(): JComponent = component()

    override fun apply(conn: KafkaConnectionData) = Unit

    override fun isModified(conn: KafkaConnectionData) = false
}
