package org.ditank.kafka.storage.configuration

final case class KafkaStorageConfiguration(
                                     bootstrapServer: String,
                                     schemaRegistryUrl: String,
                                     storeTopic: String
                                   )
