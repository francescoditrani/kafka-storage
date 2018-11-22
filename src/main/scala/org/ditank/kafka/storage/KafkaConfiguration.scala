package org.ditank.kafka.storage


final case class StreamsConfiguration(applicationId: String,
                                      replicationFactor: Int,
                                      storeName: String)

final case class KafkaConfiguration(bootstrapServer: String,
                                    schemaRegistryUrl: String,
                                    innerTopic: String,
                                    streams: StreamsConfiguration)
