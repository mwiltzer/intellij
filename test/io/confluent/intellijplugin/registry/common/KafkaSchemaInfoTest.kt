package io.confluent.intellijplugin.registry.common

import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class KafkaSchemaInfoTest {

    @Test
    fun `should have loaded details when format and version are known`() {
        assertTrue(KafkaSchemaInfo("orders-value", type = KafkaRegistryFormat.AVRO, version = 3).hasLoadedDetails())
    }

    @Test
    fun `should not have loaded details when it only carries a name`() {
        assertFalse(KafkaSchemaInfo("orders-value").hasLoadedDetails())
    }

    @Test
    fun `should not have loaded details when the format is known but not the version`() {
        assertFalse(KafkaSchemaInfo("orders-value", type = KafkaRegistryFormat.AVRO).hasLoadedDetails())
    }

    @Test
    fun `should not have loaded details when the load failed`() {
        assertFalse(KafkaSchemaInfo.createEmpty("orders-value").hasLoadedDetails())
    }
}
