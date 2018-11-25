package org.ditank.kafka.storage

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import scala.concurrent.{Future, Promise}

object RichKafkaProducer {

  implicit class AsyncKafkaProducer[K, V](producer: KafkaProducer[K, V]){

    def sendAsync(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
      val promise: Promise[RecordMetadata] = Promise[RecordMetadata]()
      val futureResult = promise.future

      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
          if (metadata != null) promise.success(metadata)
          else promise.failure(exception)
      })

      futureResult

    }

  }

}
