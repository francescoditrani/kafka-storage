package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}

class  GKStreamsStorage[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams,
                                                                  kafkaProducer: KafkaProducer[K, V],
                                                                  inputTopic: String) extends KafkaStorage[K, V] {

  private val globalTable: ReadOnlyKeyValueStore[K, V] = streams.store(inputTopic,  QueryableStoreTypes.keyValueStore[K, V])

  def insert(record: (K, V)): java.util.concurrent.Future[RecordMetadata] = kafkaProducer.send(new ProducerRecord[K, V](inputTopic, record._1, record._2))

  def get(key: K): Option[V] = Option(globalTable.get(key))


}