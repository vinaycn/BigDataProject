package hBase

/**
  * Created by vinay on 4/18/17.
  */
object hBaseTableNames {


  //HBase Table for storing all Listing Analysis By Place
  val ListingAnalysisByPlace = "ListingsAnalysisByPlace"

  val ListingsNewyork ="NewyorkListings"

  val ListingsChicago ="ChicagoListings"
  //Coulum Family for the HBase table ListingsAnalysisByPlace

}




//Column Families Names for the listings Tables
object ListingsTable{

  val descriptionColumnName = "ListingDescription"

  val reviewsColumnName ="ListingReviews"
}


//Class has all column families for the ListingsAnalysisByPlace table
object ListingsAnalysisByPlace{

  val AveragePriceByRoomType= "AveragePriceByRoomType"



  val AveragePriceByNoOfRooms = "AveragePriceByNoOfRooms"


  val top10ListingsReviewsByReviews = "Top10ListingsByReviews"


  val noOflistingsByReviewScoreRange = "NumberOfLitingsByReviewScoreRange";

  val sentimentAnalysis = "SentimentAnalysis";

}


//Class has all columns for AveragePriceByRoomType column family
object AveragePriceByRoomType{

  val EntireHomeApt ="Entire home/apt"
  val SharedRoom ="Shared room"
  val PrivateRoom ="Private room"

}

//Class has all columns for AveragePriceByNoOfRooms column family
object  AveragePriceByNoOfRooms{
  val one = "one"
  val two = "two"
  val three = "three"
  val fourPlus = "fourPlus"
}



object ListingDescription{
  val listingUrl = "ListingUrl"

  val hostName = "PictureUrl"

  val picttureUrl ="HostName"
}


object ListingReviews{

  val reviewPerMonth = "ReviewsPerMonth"

}


//All coulmns for this column family
object Top10ListingsByReviews{


  //val allcoumns = Array("first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eigth", "Ninth", "Tenth")

  val first = "first"
    val second = "second"
      val third = "third"
  val fourth = "fourth"
  val fifth = "fifth"
  val sixth  = "sixth"
  val seventh ="seventh"
  val eigth =  "eigth"
 val ninth =  "Ninth"
 val tenth =  "Tenth"

}

