package dal

import javax.inject.Inject

import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

/**
  * Created by vinay on 4/7/17.
  */
abstract class DAL (databaseConfig: DatabaseConfigProvider) {

  lazy val dbConfig = databaseConfig.get[JdbcProfile]

}
