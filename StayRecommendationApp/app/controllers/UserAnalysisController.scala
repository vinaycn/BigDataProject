package controllers

import javax.inject.{Inject, Singleton}

import hBase.{AverageAnalysisOfListing, hBaseTableData}
import kafka.utils.CoreUtils
import play.api.libs.iteratee.Enumeratee
import play.api.mvc._
import play.api.libs.json.{JsString, JsValue, Json}
/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class UserAnalysisController @Inject() (averageAnalysisOfListing: AverageAnalysisOfListing)(hBaseTableValues : hBaseTableData) extends Controller{


  def getAnalysisForStayType = Action{ implicit  request =>

    val message :Option[JsValue] = request.body.asJson

    val city = message.map{
      jsValue  => (jsValue \ "city").as[JsString].value
    }

       val result = averageAnalysisOfListing.getAverageAnalysisOfPriceByRoomType(city.get)


    Ok(Json.toJson(result))
   }



  def getAnalysisByNoOfRooms = Action{ implicit request =>


    val message :Option[JsValue] = request.body.asJson

    val city = message.map{
      jsValue  => (jsValue \ "city").as[JsString].value
    }

    val result = averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms(city.get)


    Ok(Json.toJson(result))
  }


  def getNoOfListingsByReviewScoreRange = Action{ implicit request =>


    val message :Option[JsValue] = request.body.asJson

    val city = message.map{
      jsValue  => (jsValue \ "city").as[JsString].value
    }

    val result = averageAnalysisOfListing.getNumberOfListingsByReviewScoreRange(city.get)


    Ok(Json.toJson(result))
  }


  def getTop10Listings = Action {
    print("Hello getting Top 10")
    val allListings = averageAnalysisOfListing.getTop10ListingsByReviews("Newyork")
    println(allListings)
    Ok(Json.toJson(allListings))
  }




}
