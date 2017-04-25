package actors


import actors.ConsumerActor.{ConsumeRecordsFromKafka, ReadDataFromKafka}
import actors.KafkaConsumerClientManagerActor.{GetRecommendation, RecommendedListing}
import akka.actor.{Actor, ActorRef, Props}
import kafka.{KafkaClientRecommendationRequestProducer, KafkaClientRecommendationResponseConsumer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

import scala.collection.mutable

/**
  * Created by Vinay on 4/13/17.
  */

object KafkaConsumerClientManagerActor {

  case class GetRecommendation(userId: String)

  case class RecommendedListing(values: Iterable[ConsumerRecord[String, String]])

}

//Will request another actor to consume records from the kafka
class KafkaConsumerClientManagerActor(consumer: ActorRef,kafkaProducer: KafkaClientRecommendationRequestProducer) extends Actor {

  override def preStart() = {
    println("--------------------------in prestart!!!")
    context.actorOf(Props(classOf[SparkStreamingListnerActor])) ! SparkStreamingListnerActor.StartListeningToKafka(kafkaProducer)
  }

  var bufferMap = new mutable.HashMap[String, ActorRef]()

  override def receive = {
    case GetRecommendation(userId) => {
      println("inside get recommendation")
      bufferMap.put(userId, sender())
      println(consumer)
      consumer ! ReadDataFromKafka
    }
    case RecommendedListing(values) => {
      for (value <- values) yield {
        println()
        bufferMap.get(value.key()).foreach(x => {
          println("sending value to " + x);
          x ! value.value()
          bufferMap.remove(value.key())
        })

      }
    }
  }

}
  //This actor will read the recommended Data listings from the Kafka
  object ConsumerActor {

    case class ReadDataFromKafka()

    case class ConsumeRecordsFromKafka(records: ConsumerRecords[String, String])

  }
  class ConsumerActor(consumerClient: KafkaClientRecommendationResponseConsumer) extends Actor {
    var managerActor: ActorRef = null

    override def receive = {
      case ReadDataFromKafka() => {
        managerActor = sender()
        println("reading data from kafka")
        new Thread(new KafKaConsumerThread(self, consumerClient), "Kafka_consumer_thread").start()
      }

      case ConsumeRecordsFromKafka(records) => {
        import scala.collection.JavaConverters._
        managerActor ! RecommendedListing(records.asInstanceOf[ConsumerRecords[String, String]].asScala)
      }
    }

  }




