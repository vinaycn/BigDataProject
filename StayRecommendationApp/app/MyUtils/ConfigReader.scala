package MyUtils

import javax.inject.Singleton

import com.google.inject.Inject
import play.api.Configuration

/**
  * Created by vinay on 4/11/17.
  */
@Singleton
case class ConfigReader @Inject() (configuration : Configuration) {

  val kafkaurl = configuration.getString("kafka.servers").toString
  val kafkaTopic = configuration.getString("kafka.topic").toString


  //Tweets
  val tweetApi:Option[String]= configuration.getString("twitter.apiKey")
  val tweetApiSecret:Option[String]=configuration.getString("twitter.apiSecret")
  val tweetApiToken:Option[String] =configuration.getString("twitter.token")
  val tweetApiTokenSecret:Option[String] =configuration.getString("twitter.tokenSecret")



}
