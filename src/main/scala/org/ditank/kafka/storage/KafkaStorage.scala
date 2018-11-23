package org.ditank.kafka.storage

import java.util.concurrent.CompletableFuture

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.RecordMetadata


trait KafkaStorage[K <: SpecificRecord, V <: SpecificRecord] {

  def insert(record: (K, V)): java.util.concurrent.Future[RecordMetadata]

  def get(key: K): Option[V]

  def compareUpdate(key: K, updateWith: V => Option[V]): java.util.concurrent.Future[RecordMetadata] = {
    get(key).flatMap(updateWith) match {
      case Some(value) => insert((key, value))
      case None => CompletableFuture.completedFuture(null)
    }
  }

}
