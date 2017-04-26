package actors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import hBase.MapReduceAnalysisResults
import kafka.KafkaClientRecommendationRequestProducer
import play.api.libs.json.{JsValue, Json}

/**
  * Created by vinay on 4/11/17.
  */
object RecommendationWebSocketActor {
  def props(out: ActorRef, kafkaProducer: KafkaClientRecommendationRequestProducer,
            kafkaClientManagerActor: ActorRef, user:String,averageAnalysisOfListing: MapReduceAnalysisResults) =
    Props(new RecommendationWebSocketActor(out, kafkaProducer, kafkaClientManagerActor,user,averageAnalysisOfListing))
}

class RecommendationWebSocketActor(val out: ActorRef, val kafkaProducer: KafkaClientRecommendationRequestProducer,
                                   val kafkaClientManagerActor: ActorRef, val user : String,val averageAnalysisOfListing: MapReduceAnalysisResults) extends Actor {

  def receive = {
    case msg: JsValue => {

      println("inside actor Which will publish a message")
      println(msg)
      println(msg.toString())
      kafkaProducer.publishMessage(user, msg.toString())
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
      //Getting  Recommended Listings
      val recommendedListings = averageAnalysisOfListing.getListingDetailsForRecommendation(msg)

      println("List in String" +Json.toJson(recommendedListings))
      //Convert that to Json
      out ! Json.toJson(recommendedListings)
      //out ! recommendedListings

      self ! PoisonPill
    }
  }
}
