package org.ditank.kafka.storage.configuration

final case class StreamsConfiguration(applicationId: String,
                                      replicationFactor: Int,
                                      storeName: String)

final case class ProducerConfiguration(clientId: String)

final case class KafkaConfiguration(bootstrapServer: String,
                                    schemaRegistryUrl: String,
                                    innerTopic: String,
                                    streams: StreamsConfiguration,
                                    producer: ProducerConfiguration)