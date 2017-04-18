package actors

import javax.inject.Inject

import actors.LoginActor.{AddUser, GetUser}
import akka.actor._
import akka.pattern.pipe
import dal.UserDalImpl
import models.User
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by vinay on 4/9/17.
  */


object LoginActor
{





  case class GetUser(email : String, password: String)


  case class AddUser(user : User)
}




class LoginActor (userDalImpl: UserDalImpl) extends Actor {




  override def receive: Receive = {
    case GetUser(email,password) => {
       pipe(userDalImpl.getUser(email, password)) to sender()
    }
    case AddUser(newUser) => {
      pipe(userDalImpl.addUser(newUser)) to sender()
    }
    case "WedSocket" => {
      val i = 1;
      sender() ! "Fuck u WebSocket"
    }


  }





}

