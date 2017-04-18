package controllers

import javax.inject._

import actors.LoginActor
import akka.actor.{ActorSystem, Props}
import akka.routing.RoundRobinPool
import dal.UserDalImpl
import models.{FormData, User}
import play.api._
import play.api.data.Form
import play.api.data.Forms._
import play.api.mvc._
import play.api.i18n.Messages.Implicits._
import play.api.i18n.{I18nSupport, MessagesApi}
import MyUtils.{ConfigReader, UserExceptions}
import akka.pattern.ask
import akka.actor._
import akka.stream.Materializer
import akka.util.Timeout
import hBase.hBase
import play.api.libs.streams.ActorFlow

import scala.concurrent.duration._
import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.streams._

@Singleton
class HomeController @Inject()(val messagesApi: MessagesApi)(userDalImpl: UserDalImpl)(config : ConfigReader)(implicit ec: ExecutionContext, actorSystem: ActorSystem, materializer: Materializer) extends Controller with I18nSupport {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */


  implicit val timeout: Timeout = Timeout(5 seconds)

  val loginActors = actorSystem.actorOf(Props(classOf[LoginActor],userDalImpl).withRouter(RoundRobinPool(10)),name = "LoginActors")



  def home = Action {
    Ok(views.html.index(FormData.userForm)(FormData.createUserForm)(""))
  }






  def userLogin = Action.async { implicit request =>

    FormData.userForm.bindFromRequest().fold(
      errorMsg => Future.successful(Ok(views.html.index(FormData.userForm)(FormData.createUserForm)("Something Went Wrong"))),
      userTuple => {
        loginActors ? LoginActor.GetUser(userTuple._1,userTuple._2) map(x => x match {
          case Some(user) => Ok(views.html.userMain("Welcome User")).withSession("user"-> userTuple._1)
          case None => Ok(views.html.index(FormData.userForm)(FormData.createUserForm)("InValid Credentials"))
        })
      }
    )
  }


  //

  def createUser = Action.async { implicit request =>
    FormData.createUserForm.bindFromRequest().fold(
      errorForm => Future.successful(Ok),
      user => {
        val user1 = User(0,user.name,user.email,user.age,user.password)
        loginActors ? LoginActor.AddUser(user1) map(someMes => someMes match {
            case UserExceptions.emailAlreadyExists=> Ok(views.html.index(FormData.userForm)(FormData.createUserForm)("Email Id Already Exists"))
            case UserExceptions.genericExceptioons => Ok(views.html.index(FormData.userForm)(FormData.createUserForm)("Unable to create an Account. Try After Sometime"))
            case _ => Ok(views.html.index(FormData.userForm)(FormData.createUserForm)("Please login Now"))
          })
          }
    )

  }












}
