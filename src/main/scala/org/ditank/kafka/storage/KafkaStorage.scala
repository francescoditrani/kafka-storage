package org.ditank.kafka.storage

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.ditank.kafka.storage.RichKafkaProducer._
import org.ditank.kafka.storage.builder.RichKStreamsBuilder.StorageKStreamsBuilder
import org.ditank.kafka.storage.builder.StreamsStorageBuilder
import org.ditank.kafka.storage.configuration.KafkaStorageConfiguration

import scala.concurrent.{ExecutionContext, Future}

final case class KafkaStorage[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams,
                                                                        kafkaProducer: KafkaProducer[K, V],
                                                                        storeName: String) {

  private val globalTable: ReadOnlyKeyValueStore[K, V] = streams.store(storeName, QueryableStoreTypes.keyValueStore[K, V])

  def insert(record: (K, V)): Future[RecordMetadata] = kafkaProducer.sendAsync(new ProducerRecord[K, V](storeName, record._1, record._2))

  def get(key: K): Option[V] = Option(globalTable.get(key))

  def compareUpdate(key: K, mapOldValue: Option[V] => Option[V])(implicit ec: ExecutionContext): Future[Option[RecordMetadata]] =
    mapOldValue(get(key)) match {
      case Some(value) => insert(key, value).map(Some(_))
      case None => Future.successful(None)
    }

}


object KafkaStorage {

  def apply[K <: SpecificRecord, V <: SpecificRecord](kafkaConf: KafkaStorageConfiguration): KafkaStorage[K, V] = {

    val streamsProps: Properties = new Properties()
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, s"kafka-storage-kstreams-applicationid-${kafkaConf.storeTopic}")
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConf.bootstrapServer)
    streamsProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConf.schemaRegistryUrl)
    streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = new StreamsBuilder()
    val storageBuilder: StreamsStorageBuilder = builder.storageBuilder(kafkaConf)
    val topology: Topology = builder.build()

    val streams = new KafkaStreams(topology, streamsProps)

    streams.start()

    storageBuilder.build(streams, kafkaConf)

  }

}