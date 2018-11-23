package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait KafkaStorage[K <: SpecificRecord, V <: SpecificRecord] {

  def insert(record: (K, V)): Future[Unit]

  def get(key: K): Option[V]

  def compareUpdate(key: K, updateWith: V => Option[V]): Future[Option[V]] = {
    get(key).flatMap[V](updateWith) match {
      case Some(value) => insert((key, value)).map(_ => Some(value))
      case None => Future(None)
    }
  }

}
