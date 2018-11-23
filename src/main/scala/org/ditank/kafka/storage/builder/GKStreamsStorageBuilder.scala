package org.ditank.kafka.storage.builder

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.streams.KafkaStreams
import org.ditank.kafka.storage.configuration.KafkaStorageConfiguration
import org.ditank.kafka.storage.{GKStreamsStorage, KafkaStorage}

class GKStreamsStorageBuilder() {

  def build[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams, kafkaConfiguration: KafkaStorageConfiguration): KafkaStorage[K, V] = {
    val kafkaProducer = new KafkaProducer[K, V](conf(kafkaConfiguration))
    new GKStreamsStorage(streams, kafkaProducer, kafkaConfiguration.storeTopic)
  }

  private def conf[K <: SpecificRecord, V <: SpecificRecord](kafkaConfiguration: KafkaStorageConfiguration) = {
    val config = new Properties()
    config.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaConfiguration.storeTopic)
    config.put(ProducerConfig.ACKS_CONFIG, "all")
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.bootstrapServer)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[SpecificAvroSerializer[K]])
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[SpecificAvroSerializer[V]])
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConfiguration.schemaRegistryUrl)
    config
  }

}
