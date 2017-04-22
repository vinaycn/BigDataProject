package controllers

import javax.inject.{Inject, Singleton}

import hBase.AverageAnalysisOfListing
import kafka.utils.CoreUtils
import play.api.libs.iteratee.Enumeratee
import play.api.mvc._
import play.api.libs.json.Json
/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class UserAnalysisController @Inject() (averageAnalysisOfListing: AverageAnalysisOfListing) extends Controller{


  def graph = Action{

    println("Im here")
       val maps= averageAnalysisOfListing.getAverageAnalysisOfPriceByRoomType("Berlin")
       //val mapsforRooms = averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms("Berlin")

      //val somesd =  maps += mapsforRooms
       println(maps)
       //maps.toList.foreach(x => println(x))
        val some = Json.toJson(maps)
       Ok(Json.toJson(maps))
   }



  def graph1 = Action{

    //val maps= averageAnalysisOfListing.getAverageAnalysisOfPriceByRoomType("Berlin")
    val mapsforRooms = averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms("Berlin")
    print(mapsforRooms)
    //println(maps)
    mapsforRooms.toList.foreach(x => println(x))
    //val some = Json.toJson(mapsforRooms)
    Ok(Json.toJson(mapsforRooms))
  }


}
