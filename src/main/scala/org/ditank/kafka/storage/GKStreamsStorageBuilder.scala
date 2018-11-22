package org.ditank.kafka.storage

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.streams.KafkaStreams

class GKStreamsStorageBuilder() {

  def build[K <: SpecificRecord, V <: SpecificRecord](streams: KafkaStreams): KafkaStorage[K, V] =
    new GKStreamsStorage(streams)

}
