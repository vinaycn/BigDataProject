package actors

import actors.ConsumerActor.ReadDataFromKafka
import actors.KafkaConsumerClientManagerActor.{GetRecommendation, RecommendedListing}
import akka.actor.{Actor, ActorRef}
import kafka.KafkaClientRecommendationResponseConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable

/**
  * Created by akashnagesh on 4/13/17.
  */

object KafkaConsumerClientManagerActor {

  case class GetRecommendation(userId: String)

  case class RecommendedListing(values: Iterable[ConsumerRecord[String, String]])

}

class KafkaConsumerClientManagerActor(consumer: ActorRef) extends Actor {

  var bufferMap = new mutable.HashMap[String, ActorRef]()

  override def receive = {
    case GetRecommendation(userId) => {
      println("inside get recommendation")
      bufferMap.put(userId, sender())
      println(consumer)
      consumer ! ReadDataFromKafka
    }
    case RecommendedListing(values) => {
      println("got recommended listing")
      for (value <- values) yield {
        bufferMap.get(value.key()).foreach(x => x ! value.value())
        bufferMap.remove(value.key())
      }
    }
  }
}


object ConsumerActor {

  case class ReadDataFromKafka()

}

class ConsumerActor(consumerClient: KafkaClientRecommendationResponseConsumer) extends Actor {
  var valueFromKafka: Iterable[ConsumerRecord[String, String]] = null

  override def receive = {
    case ReadDataFromKafka => {
      var foundRecord = false
      println("reading data from kafka")
      while (!foundRecord) {
        valueFromKafka = consumerClient.consumeMessage()
        println(valueFromKafka.isEmpty)
        println(valueFromKafka)
        if (!valueFromKafka.isEmpty) {
          foundRecord = true
        }
      }
      // consumerClient.kConsumer.commitSync()
      println("=============value is" + valueFromKafka.size)
      sender() ! RecommendedListing(valueFromKafka)
    }
  }
}


