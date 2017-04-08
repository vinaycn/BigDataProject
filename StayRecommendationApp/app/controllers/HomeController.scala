package controllers

import javax.inject._

import dal.UserDalImpl
import models.{FormData, User}
import play.api._
import play.api.data.Form
import play.api.data.Forms._
import play.api.mvc._
import play.api.i18n.Messages.Implicits._
import play.api.i18n.{I18nSupport, MessagesApi}
import utils.UserExceptions

import scala.concurrent.{ExecutionContext, Future}

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(val messagesApi: MessagesApi)(userDalImpl: UserDalImpl)(implicit ec: ExecutionContext) extends Controller with I18nSupport {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */



  def home = Action {
    Ok(views.html.index(FormData.userForm)(FormData.createUserForm)(""))
  }

  //def home():Action[AnyContent] = this.index(" ")


  def userLogin = Action{ implicit request =>
    FormData.userForm.bindFromRequest().fold(
      errorMsg => BadRequest,
      userTuple => Ok(s"User ${userTuple._1} logged in successfully")
    )
  }

  def createUser = Action.async { implicit request =>
    FormData.createUserForm.bindFromRequest().fold(
      errorForm => Future.successful(Ok),
      user => {
        val user1 = User(0,user.name,user.email,user.age,user.password)
        userDalImpl.addUser(user1)
          .map(someMes => someMes match {
            case UserExceptions.emailAlreadyExists=> Ok(views.html.index(FormData.userForm)(FormData.createUserForm)("Email Id Already Exists"))
           // case Exception => Redirect(routes.HomeController.index("Unable to create an Account. Try After Sometime"))
            case _ => Ok("New Page will come here ")
          })
          }
    )

  }





}
