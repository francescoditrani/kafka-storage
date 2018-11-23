package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.{ExecutionContext, Future}


trait KafkaStorage[K <: SpecificRecord, V <: SpecificRecord] {

  def insert(record: (K, V)): Future[RecordMetadata]

  def get(key: K): Option[V]

  def compareUpdate(key: K, mapOldValue: Option[V] => Option[V])(implicit ec: ExecutionContext): Future[Option[RecordMetadata]] =
    mapOldValue(get(key)) match {
      case Some(value) => insert(key, value).map(Some(_))
      case None => Future.successful(None)
    }

}
