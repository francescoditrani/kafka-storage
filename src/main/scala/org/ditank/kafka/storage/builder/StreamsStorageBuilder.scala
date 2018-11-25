package org.ditank.kafka.storage.builder

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.streams.KafkaStreams
import org.ditank.kafka.storage.KafkaStorage
import org.ditank.kafka.storage.configuration.KafkaStorageConfiguration

final class StreamsStorageBuilder() {

  def build[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams, kafkaConfiguration: KafkaStorageConfiguration): KafkaStorage[K, V] = {
    val kafkaProducer = new KafkaProducer[K, V](conf(kafkaConfiguration))
    new KafkaStorage(streams, kafkaProducer, kafkaConfiguration.storeTopic)
  }

  private def conf[K <: SpecificRecord, V <: SpecificRecord](kafkaConf: KafkaStorageConfiguration) = {
    val config = new Properties()
    config.put(ProducerConfig.CLIENT_ID_CONFIG, s"kafka-storage-producer-${kafkaConf.storeTopic}")
    config.put(ProducerConfig.ACKS_CONFIG, "all")
//    config.put(ProducerConfig.BATCH_SIZE_CONFIG, "0")
//    config.put("max.in.flight.requests.per.connection", "1")
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConf.bootstrapServer)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[SpecificAvroSerializer[K]])
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[SpecificAvroSerializer[V]])
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConf.schemaRegistryUrl)
    config
  }

}
