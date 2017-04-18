package actors

import akka.actor.Actor

/**
  * Created by vinay on 4/18/17.
  */

object HBaseClientActor{
  case class AveragePriceByRoomType(place:String)

  case class AveragePriceByNoOfRooms(place:String)
}


class HBaseClientActor extends Actor {
  override def receive: Receive = ???
}
