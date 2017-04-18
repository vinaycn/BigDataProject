package kafka

import java.util
import java.util.Properties
import javax.inject.{Inject, Singleton}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import play.api.Configuration

/**
  * Created by Vinay on 4/12/17.
  */
@Singleton
class KafkaClientRecommendationRequestProducer @Inject()(conf: Configuration) {

  val kproducer = new KafkaProducer[String, String](initializeProperties(KafkaClientSerializers.STRING_SERIALIZER, KafkaClientSerializers.STRING_SERIALIZER))

  def publishMessage(key: String, value: String) = {
    val rec = new ProducerRecord[String, String]("topic3", key, value)
    kproducer.send(rec)
  }

  private def initializeProperties(keySerializer: String, valueSerializer: String): Properties = {

    val bootStrapServer = conf.getString("kafka.bootstrap.servers").getOrElse("no bootstrap server in app config")
    val producerProperties = new Properties()
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer)
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    producerProperties
  }

}

object KafkaClientSerializers {
  val BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer"
  val STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
}

@Singleton
class KafkaClientRecommendationResponseConsumer @Inject()(conf: Configuration) {

  val kConsumer = new KafkaConsumer[String, String](initializeProperties(KafkaClientDeSerializers.STRING_DESERIALIZER, KafkaClientDeSerializers.STRING_DESERIALIZER))
  kConsumer.subscribe(util.Arrays.asList("topic3"))

  def consumeMessage() = {
    import scala.collection.JavaConverters._
    kConsumer.poll(1000).asScala
  }

  private def initializeProperties(keyDeSerializer: String, valueDeSerializer: String): Properties = {

    val bootStrapServer = conf.getString("kafka.bootstrap.servers").getOrElse("no bootstrap server in app config")
    val consumerProperties = new Properties()
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer)
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerializer)
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer)
    //consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    //hardcodded because there is only one group of consumer
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "recommendationConsumer")
    consumerProperties
  }

}

object KafkaClientDeSerializers {
  val STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer"
}


