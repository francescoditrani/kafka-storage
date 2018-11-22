package org.ditank.kafka.storage

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.Stores

object RichKStreamsBuilder {

  implicit class StoragedKStreamsBuilder(builder: StreamsBuilder) {

    def storageBuilder(): GKStreamsStorageBuilder = {

      val mat = Materialized.as[K, V](Stores.persistentKeyValueStore(kafkaConf.streams.storeName))
        .withKeySerde(streamSerdes.latestPartitionOffsetKey)
        .withValueSerde(streamSerdes.latestPartitionOffset)

      builder.globalTable(config.innerTopic, mat)

      new GKStreamsStorageBuilder()
      //create store
      //attach the store
      //we return the storageBuilder
    }


  }

}
