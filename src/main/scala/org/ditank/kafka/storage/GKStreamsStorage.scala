package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}

import scala.concurrent.Future

class  GKStreamsStorage[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams,
                                                                  kafkaProducer: KafkaProducer[K, V],
                                                                  inputTopic: String) extends KafkaStorage[K, V] {

  private val globalTable: ReadOnlyKeyValueStore[K, V] = streams.store(inputTopic,  QueryableStoreTypes.keyValueStore[K, V])

  def insert(record: (K, V)): Future[Unit] = ???

  def get(key: K): Option[V] = Option(globalTable.get(key))


}