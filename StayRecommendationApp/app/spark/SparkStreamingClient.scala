
package spark


import javax.inject.Singleton


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by vinay on 4/17/17.
  */



@Singleton
class SparkStreamingClient {


  //  val conf = Play.current.configuration
  val bootStrapServer = "localhost:9092" //conf.getString("kafka.bootstrap.servers").getOrElse("no bootstrap server in app config")
  println("inside this classss +++++++++++++++++++++" + bootStrapServer)

  val userPreferenceTopic = "requestRecommendation" //conf.getString("kafka.topicIn").getOrElse("no input topic")
  val sparkAppName = "StayRecommendation"
  //conf.getString("spark.appName").getOrElse("no spark application name")
  val master = "local[*]"
  //conf.getString("spark.master").getOrElse("no spark master")
  val consumerGroupId = "sparkConsumer" //conf.getString("streamconsumer.groupid").getOrElse("no group id for consumer")

  val kafkaParams = Map[String, Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServer,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> consumerGroupId,
    "auto.offset.reset" -> "latest")

  val sc = SparkCommons.sc

  val ssc = SparkCommons.streamingContext


  val listingPredictor = new ListingPredictor(sc)



  val kafkaReceiverParams = Map[String, String](
    "metadata.broker.list" -> "192.168.10.2:9092")

  val topics = Array("requestRecommendation")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
  val mapped = stream.map(record => (record.key(), record.value()))

  println("---------------------------")
  val x = mapped.foreachRDD { x => {
    val l = x.collect()
    println("number of elements received in 4 seconds is " + l.length)
    l.foreach(individualRecord => println("listing suitable ++++++++++++" + listingPredictor.recommendListing("111")))
  }
  }
  ssc.start()
  ssc.awaitTermination()

}

