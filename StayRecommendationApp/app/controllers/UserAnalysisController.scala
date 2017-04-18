package controllers

import javax.inject.{Inject, Singleton}

import hBase.AverageAnalysisOfListing
import kafka.utils.CoreUtils
import play.api.mvc._
import play.api.libs.json.Json
/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class UserAnalysisController @Inject() (averageAnalysisOfListing: AverageAnalysisOfListing) extends Controller{


  def graph = Action{

       val maps= averageAnalysisOfListing.getAverageAnalysisOfPriceByPlace("Berlin")
       println(maps)
       maps.toList.foreach(x => println(x))
        val some = Json.toJson(maps)
       Ok(Json.toJson(maps))
   }



}
