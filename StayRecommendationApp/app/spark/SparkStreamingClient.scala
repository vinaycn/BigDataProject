
package spark

import java.util
import java.util.{Collection, Collections, HashMap, Map}

import kafka.{KafkaClientDeSerializers, KafkaClientSerializers}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import play.api.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.streaming
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by vinay on 4/17/17.
  */
class SparkStreamingClient(configuration : Configuration) {


  val kafkaServers: String = configuration.getString("kafka.servers").getOrElse("No kafka Server Provided")
  val readTopic: String = configuration.getString("kafka.Usertopic").getOrElse("No Topic to read Data")
  val writeTopic = configuration.getString("kafka.RecommendationTopic").getOrElse("No topic for write an data")
  val appName: String = configuration.getString("StayRecoomendationApp").getOrElse("StayRecommendationApp")
  val sparkMaster: /**/String = configuration.getString("local[*]").getOrElse("local[*]")


  /*val kafkaParams = Map{"bootstrap.servers" ->kafkaServers;
    "key.deserializer" -> KafkaClientDeSerializers.STRING_DESERIALIZER;
    "value.deserializer"-> KafkaClientDeSerializers.STRING_DESERIALIZER;
    "group.id"->"SparkStreaming";
    "auto.offset.reset"->"latest";
    "enable.auto.commit"->false
  }*/


 /* kafkaParams.put("bootstrap.servers", kafkaServers)
  kafkaParams.put("key.deserializer", classOf[StringDeserializer])
  kafkaParams.put("value.deserializer", classOf[StringDeserializer])
  kafkaParams.put("group.id", groupId)
  kafkaParams.put("auto.offset.reset", "latest")
  kafkaParams.put("enable.auto.commit", false)*/

  val conf: SparkConf = new SparkConf().setMaster(sparkMaster).setAppName(appName)
  val sc = new SparkContext(conf)
  val streamingContext = new StreamingContext(conf,Seconds(5))


 // val topics: util.Collection[String] = Collections.singletonList(readTopic)

  //val stream: JavaInputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

  //val records: JavaDStream[String] = stream.map(ConsumerRecord.value.asInstanceOf[Function[ConsumerRecord[String, String], String]])

  //ecords.print()

  streamingContext.start()
  streamingContext.awaitTermination()

}

