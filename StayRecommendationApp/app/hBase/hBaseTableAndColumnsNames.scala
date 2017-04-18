package hBase

/**
  * Created by vinay on 4/18/17.
  */
object hBaseTableNames {


  //HBase Table for storing all Listing Analysis By Place
  val ListingAnalysisByPlace = "ListingsAnalysisByPlace"

  //Coulum Family for the HBase table ListingsAnalysisByPlace

}



//Class has all column families for the ListingsAnalysisByPlace table
object ListingsAnalysisByPlace{

  val AveragePriceByRoomType= "AveragePriceByRoomType"



  val AveragePriceByNoOfRooms = "AveragePriceByNoOfRooms"

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
