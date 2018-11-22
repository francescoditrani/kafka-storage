package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.streams.KafkaStreams

import scala.concurrent.Future

class GKStreamsStorage[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams) extends KafkaStorage[K, V] {

  def insert(record: (K, V)): Future[Unit] = ???

  def get(key: K): Future[Option[V]] = ???


}