package dal

import javax.inject.Singleton

import com.google.inject.{ImplementedBy, Inject}
import models.User
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.MySQLDriver.api._
import MyUtils.UserExceptions
import akka.pattern.pipe
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
/**
  * Created by vinay on 4/7/17.
  */

trait UserDal {
def addUser(user: User):Future[String]
  def getUser(email : String,password: String):Future[Option[User]]

}


@Singleton
class UserDalImpl @Inject() (databaseConfig: DatabaseConfigProvider)  extends DAL(databaseConfig)  with UserDal{


  import dbConfig._

  private class UserTableDef(tag :Tag) extends Table[User](tag,"user"){

    def id = column[Long]("id",O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def email = column[String]("email")

    def age = column[Int]("age")

    def password = column[String]("password")

    def emailIndex = index("email_user",email,unique = true)

    override def * = (id,name,email,age,password) <> (User.tupled,User.unapply)

  }


  private val users = TableQuery[UserTableDef]

  override def addUser(user: User): Future[String] =  {
    db.run(users += user).map(res => "User Added Successfully").recover{
      case emailException:com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException => UserExceptions.emailAlreadyExists
      case ex:Exception => UserExceptions.genericExceptioons
    }
  }

  override def getUser(email: String, password: String): Future[Option[User]] = {
    db.run{
      users.filter(_.email === email).filter(_.password === password).result.headOption
  }

  }



}
