package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord

import scala.concurrent.Future

trait Storage[K <: SpecificRecord, V <: SpecificRecord] {

  def insert(record: (K, V)): Future[Unit]

  def get(key: K): Future[Option[V]]

  def compareUpdate(key: K, updateWith: V => Option[V]): Future[Option[V]] = {
    get(key)
      .map(_.flatMap(updateWith))
      .flatMap {
        case Some(value) => insert((key, value)).map(_ => Some(value))
        case None => Future(None)
      }
  }

}