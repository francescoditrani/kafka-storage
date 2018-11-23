package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.ditank.kafka.storage.RichKafkaProducer._

import scala.concurrent.Future

class  GKStreamsStorage[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams,
                                                                  kafkaProducer: KafkaProducer[K, V],
                                                                  inputTopic: String) extends KafkaStorage[K, V] {

  private val globalTable: ReadOnlyKeyValueStore[K, V] = streams.store(inputTopic,  QueryableStoreTypes.keyValueStore[K, V])

  def insert(record: (K, V)): Future[RecordMetadata] = kafkaProducer.sendAsync(new ProducerRecord[K, V](inputTopic, record._1, record._2))

  def get(key: K): Option[V] = Option(globalTable.get(key))


}