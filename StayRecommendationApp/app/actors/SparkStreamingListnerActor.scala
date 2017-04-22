package actors

import actors.SparkStreamingListnerActor.StartListeningToKafka
import akka.actor.Actor
import spark.SparkStreamingClient

/**
  * Created by Vinay on 4/18/17.
  */

object SparkStreamingListnerActor {

  case class StartListeningToKafka()

}

//This actor will create SparlStreaming Instance
class SparkStreamingListnerActor extends Actor {
  override def receive: Receive = {
    case StartListeningToKafka() => {
      println("++++++++++++++++++==========++++===+++++==++==+=++====+=+=++=++==+starting to listening to kafka")
      val sparkClient = new SparkStreamingClient
    }
  }
}
