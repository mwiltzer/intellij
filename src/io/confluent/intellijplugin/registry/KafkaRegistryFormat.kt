package io.confluent.intellijplugin.registry

import io.confluent.intellijplugin.util.KafkaMessagesBundle
import org.jetbrains.annotations.Nls

enum class KafkaRegistryFormat(@Nls val presentable: String) {
    AVRO(KafkaMessagesBundle.message("registry.format.avro")),
    PROTOBUF(KafkaMessagesBundle.message("registry.format.protobuf")),
    JSON(KafkaMessagesBundle.message("registry.format.json")),
    UNKNOWN(KafkaMessagesBundle.message("registry.format.unknown"));

    companion object {
        fun parse(s: String?) = entries.firstOrNull { it.name.lowercase() == s?.lowercase() } ?: UNKNOWN

        /**
         * Map Schema Registry schema type string to format enum.
         * Per SR convention, null schemaType defaults to AVRO.
         * Unrecognized types return UNKNOWN.
         */
        fun fromSchemaType(type: String?): KafkaRegistryFormat = when (type?.uppercase()) {
            null, "AVRO" -> AVRO  // null defaults to AVRO per SR convention
            "PROTOBUF" -> PROTOBUF
            "JSON" -> JSON
            else -> UNKNOWN
        }

        /**
         * Like [fromSchemaType], for a schema whose details are loaded separately from its name (Confluent Cloud).
         * A missing type only means Avro once the details were loaded, which is when the schema has a version.
         * Before that nothing is known about the format, and calling it Avro would label every schema that way.
         */
        fun fromLoadedSchemaType(version: Int?, type: String?): KafkaRegistryFormat? =
            version?.let { fromSchemaType(type) }
    }
}