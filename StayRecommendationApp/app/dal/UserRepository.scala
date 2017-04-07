package dal

import javax.inject.{Inject, Singleton}

import models.User
import net.sf.ehcache.search.expression.And
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.openid.Errors.AUTH_CANCEL
import slick.driver.JdbcProfile

import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by vinay on 4/5/17.
  */
@Singleton
class UserRepository @Inject()(databaseConfig: DatabaseConfigProvider)(implicit ec: ExecutionContext){

  private val dbConfig = databaseConfig.get[JdbcProfile]


  import dbConfig._
  import driver.api._



  private class UserTableDef(tag :Tag) extends Table[User](tag,"user"){

    def id = column[Long]("id",O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def email = column[String]("email")

    def age = column[Int]("age")

    def password = column[String]("password")

    override def * = (id,name,email,age,password) <> ((User.apply _).tupled,User.unapply)

  }


    private val users = TableQuery[UserTableDef]


  def addUser(name: String,email: String,age :Int,password: String) : Future[User] = db.run {
      (users.map(user => (user.name,user.email,user.age,user.password))

        returning users.map(_.id)

        into ((userAll,id) => User(id,userAll._1,userAll._2,userAll._3,userAll._4))
        ) += (name,email,age,password)
    }


  def findUser(email :String,password: String) : Future[Seq[User]] = db.run{
    val user = users.result.map(user => user.filter(_.email.toString() == email).filter(_.password.toString()==password))
    user
  }




}
