package org.ditank.kafka.storage.helper

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord

object SerdeHelper {
  def createSerde[T <: SpecificRecord](isKey: Boolean, schemaRegistryUrl: String): SpecificAvroSerde[T] = {
    val serde = new SpecificAvroSerde[T]()

    val properties = new java.util.HashMap[String, String]()
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
    serde.configure(properties, isKey)
    serde

  }
}
