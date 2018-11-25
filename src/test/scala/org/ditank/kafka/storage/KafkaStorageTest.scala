package org.ditank.kafka.storage

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafkaConfigWithSchemaRegistry, EmbeddedKafkaWithSchemaRegistry}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreType, ReadOnlyKeyValueStore}
import org.ditank.kafka.storage.configuration.KafkaStorageConfiguration
import org.ditank.kafka.storage.test.{TestKey, TestValue}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, refEq}
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class KafkaStorageTest extends FlatSpec with BeforeAndAfterEach with MockitoSugar with ScalaFutures with EmbeddedKafkaWithSchemaRegistry {

  var gKStreamsStorage: KafkaStorage[TestKey, TestValue] = _
  var mockStreams: KafkaStreams = _
  var kafkaProducer: KafkaProducer[TestKey, TestValue] = _
  val storeName = "input-store-name"
  var globalTable: ReadOnlyKeyValueStore[TestKey, TestValue] = _

  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds), interval = Span(700, Millis))

  val producerRecordCaptor: ArgumentCaptor[ProducerRecord[TestKey, TestValue]] = ArgumentCaptor.forClass(classOf[ProducerRecord[TestKey, TestValue]])

  override protected def beforeEach(): Unit = {
    mockStreams = mock[KafkaStreams]
    kafkaProducer = mock[KafkaProducer[TestKey, TestValue]]
    globalTable = mock[ReadOnlyKeyValueStore[TestKey, TestValue]]
    when(mockStreams
      .store[ReadOnlyKeyValueStore[TestKey, TestValue]](refEq(storeName), any[QueryableStoreType[ReadOnlyKeyValueStore[TestKey, TestValue]]])
    ).thenReturn(globalTable)

    gKStreamsStorage = KafkaStorage[TestKey, TestValue](mockStreams, kafkaProducer, storeName)

  }

  "Get" should "return None if store returns null" in {
    val key = TestKey("test-uuid")
    when(globalTable.get(key)).thenReturn(null)
    assert(gKStreamsStorage.get(key) === None)
  }

  it should "return Some(Value) if store returns value" in {
    val key = TestKey("test-uuid")
    val value = TestValue("test-name", 1)
    when(globalTable.get(key)).thenReturn(value)
    assert(gKStreamsStorage.get(key) === Some(value))
  }

  "Insert" should "call Kafka producer send with correct parameters" in {
    val key = TestKey("test-uuid")
    val value = TestValue("test-name", 1)

    gKStreamsStorage.insert((key, value))

    verify(kafkaProducer).send(producerRecordCaptor.capture(), any[Callback])
    assert(producerRecordCaptor.getValue.key() === key)
    assert(producerRecordCaptor.getValue.value() === value)
  }

  "CompareUpdate" should "not call send and return null, if updateWith return None" in {
    val key = TestKey("test-uuid")
    val updateWith: Option[TestValue] => Option[TestValue] = _ => None
    val future = gKStreamsStorage.compareUpdate(key, updateWith)

    assert(Await.result(future, 5.seconds) === None)
    verify(kafkaProducer, never()).send(any[ProducerRecord[TestKey, TestValue]], any[Callback])
  }

  it should "call send send and return the metadata if updateWith return Value" in {
    val key = TestKey("test-uuid")
    val value = TestValue("test-name", 1)
    val newValue = TestValue("test-name", 2)

    when(globalTable.get(key)).thenReturn(value)

    val updateWith: Option[TestValue] => Option[TestValue] = _ => Some(newValue)

    gKStreamsStorage.compareUpdate(key, updateWith)
    verify(kafkaProducer).send(producerRecordCaptor.capture(), any[Callback])
    assert(producerRecordCaptor.getValue.key() === key)
    assert(producerRecordCaptor.getValue.value() === newValue)
  }

  "KafkaStorage" should "save and get a record" in {

    implicit val config: EmbeddedKafkaConfigWithSchemaRegistry = EmbeddedKafkaConfigWithSchemaRegistry()

    withRunningKafka {

      val topic = "test-topic"
      createCustomTopic(topic)

      val kafkaStorage = KafkaStorage[TestKey, TestValue](KafkaStorageConfiguration(
        "localhost:6001",
        s"http://localhost:6002",
        topic
      ))

      val key = TestKey("test-uuid")
      val key2 = TestKey("test-uuid2")
      val value = TestValue("test-name", 1)
      val value2 = TestValue("test-name2", 1)

      val result = kafkaStorage.insert((key, value))
        .map(_ => Thread.sleep(70))
        .map(_ => kafkaStorage.get(key))
        .flatMap(_ => kafkaStorage
          .insert((key2, value2))
          .map(_ => Thread.sleep(10))
          .map(_ => kafkaStorage.get(key2))
        )

      whenReady(result) {
        case Some(testValue: TestValue) => assert(testValue === value2)
        case _ => fail("No record found")
      }

    }


  }


}
