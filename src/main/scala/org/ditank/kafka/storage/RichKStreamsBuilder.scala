package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.streams.kstream.{Consumed, Materialized}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.Stores
import org.ditank.kafka.storage.SerdeHelper._

object RichKStreamsBuilder {

  implicit class StorageKStreamsBuilder(builder: StreamsBuilder) {

    def storageBuilder[K <: SpecificRecord, V <: SpecificRecord](kafkaConf: KafkaConfiguration): GKStreamsStorageBuilder = {

      implicit val consumed: Consumed[K, V] = Consumed.`with`(
        createSerde[K](true, kafkaConf.schemaRegistryUrl),
        createSerde[V](false, kafkaConf.schemaRegistryUrl)
      )

      val mat = Materialized.as[K, V](Stores.persistentKeyValueStore(kafkaConf.streams.storeName))
        .withKeySerde(createSerde[K](true, kafkaConf.schemaRegistryUrl))
        .withValueSerde(createSerde[V](false, kafkaConf.schemaRegistryUrl))

      builder.globalTable(kafkaConf.innerTopic, mat)

      new GKStreamsStorageBuilder()
      //create store
      //attach the store
      //we return the storageBuilder
    }


  }

}
