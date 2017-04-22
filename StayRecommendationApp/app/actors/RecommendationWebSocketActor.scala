package actors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import kafka.KafkaClientRecommendationRequestProducer

/**
  * Created by vinay on 4/11/17.
  */
object RecommendationWebSocketActor {
  def props(out: ActorRef, kafkaProducer: KafkaClientRecommendationRequestProducer,
            kafkaClientManagerActor: ActorRef, user:String) =
    Props(new RecommendationWebSocketActor(out, kafkaProducer, kafkaClientManagerActor,user))
}

class RecommendationWebSocketActor(val out: ActorRef, val kafkaProducer: KafkaClientRecommendationRequestProducer,
                                   val kafkaClientManagerActor: ActorRef, val user : String) extends Actor {

  def receive = {
    case msg: String => {

      println("inside actor Which will publish a message")

      kafkaProducer.publishMessage(user, msg)
      println("published the message to the kafka")
      kafkaClientManagerActor ! KafkaConsumerClientManagerActor.GetRecommendation(user)
      // out ! msg + "appending this from server"
      context.become(onConsumerMessageBehavior)
    }
  }

  def onConsumerMessageBehavior: Receive = {
    case msg: String => {
      println("final msg in actor")
      println(msg)
      out ! msg
      self ! PoisonPill
    }
  }
}
