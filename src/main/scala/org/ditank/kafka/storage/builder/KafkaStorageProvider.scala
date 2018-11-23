package org.ditank.kafka.storage.builder

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import RichKStreamsBuilder.StorageKStreamsBuilder
import org.ditank.kafka.storage.KafkaStorage
import org.ditank.kafka.storage.configuration.KafkaStorageConfiguration

class KafkaStorageProvider[K <: SpecificRecord, V <: SpecificRecord]() {

  def create(kafkaConf: KafkaStorageConfiguration): KafkaStorage[K, V] = {

    lazy val streamsProps = streamsProperties(kafkaConf)

    val builder = new StreamsBuilder()

    val storageBuilder = builder.storageBuilder(kafkaConf)

    val topology = builder.build()

    val streams = new KafkaStreams(topology, streamsProps)

    streams.start()

    storageBuilder.build(streams, kafkaConf)
  }

  private def streamsProperties(kafkaConf: KafkaStorageConfiguration) = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConf.storeTopic)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConf.bootstrapServer)
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConf.schemaRegistryUrl)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

}
