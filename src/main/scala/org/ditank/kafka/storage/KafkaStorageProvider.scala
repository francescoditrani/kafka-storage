package org.ditank.kafka.storage

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import RichKStreamsBuilder.StorageKStreamsBuilder

class KafkaStorageProvider[K <: SpecificRecord, V <: SpecificRecord]() {

  def create(kafkaConfiguration: KafkaConfiguration): KafkaStorage[K, V] = {
    lazy val streamsProps = streamsProperties(kafkaConfiguration)

    val builder = new StreamsBuilder()

    val storageBuilder = builder.storageBuilder(kafkaConfiguration)

    val topology = builder.build()

    val streams = new KafkaStreams(topology, streamsProps)

    streams.start()

    storageBuilder.build(streams, kafkaConfiguration)
  }

  private def streamsProperties(kafkaConfig: KafkaConfiguration) = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.streams.applicationId)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServer)
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConfig.schemaRegistryUrl)
    p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, kafkaConfig.streams.replicationFactor.toString)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

}
