package controllers

import javax.inject._

import models.{FormData, User}
import play.api._
import play.api.data.Form
import play.api.data.Forms._
import play.api.mvc._
import play.api.i18n.Messages.Implicits._
import play.api.i18n.{I18nSupport, MessagesApi}

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(val messagesApi: MessagesApi)  extends Controller with I18nSupport {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */



  def index = Action {
    Ok(views.html.index(FormData.userForm)(FormData.createUserForm))
  }


  def userLogin = Action{ implicit request =>
    FormData.userForm.bindFromRequest().fold(
      errorMsg => BadRequest,
      userTuple => Ok(s"User ${userTuple._1} logged in successfully")
    )
  }

  def createUser = Action{ implicit request =>
    FormData.createUserForm.bindFromRequest().fold(
      errorForm => BadRequest,
      user => Ok(s"User Created Successfully")
    )

  }





}