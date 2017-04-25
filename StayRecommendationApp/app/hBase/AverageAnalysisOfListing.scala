package hBase

import java.util
import javax.inject.{Inject, Singleton}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

import scala.compat.java8.FutureConverters
import scala.concurrent.Future

/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class AverageAnalysisOfListing {



def getAverageAnalysisOfPriceByRoomType(place : String) ={
  val columnFamily:Array[Byte] = ListingsAnalysisByPlace.AveragePriceByRoomType.getBytes
  val homeColumn:Array[Byte] = AveragePriceByRoomType.EntireHomeApt.getBytes
  val sharedColumn:Array[Byte]= AveragePriceByRoomType.SharedRoom.getBytes
  val privateColumn:Array[Byte] = AveragePriceByRoomType.PrivateRoom.getBytes
println("Fuck u play")
  //Get the Connection
  val connection = hBase.getConnection
  //Get the Table
  val tabel =  connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))
  import org.apache.hadoop.hbase.util.Bytes
  val get = new Get(Bytes.toBytes(place))
  //get.getFamilyMap;
  get.addFamily(columnFamily)

  val result = tabel.get(get)

  val homeAveragePrice:Array[Byte] = result.getValue(columnFamily,homeColumn)
  val sharedAveragePrice:Array[Byte] = result.getValue(columnFamily,sharedColumn)
  val privateAveragePrice:Array[Byte] = result.getValue(columnFamily,privateColumn)
  val ha = Bytes.toDouble(homeAveragePrice)
  val sh = Bytes.toDouble(sharedAveragePrice)
  val pv = Bytes.toDouble(privateAveragePrice)
  val aggData =Map(AveragePriceByRoomType.SharedRoom -> sh,AveragePriceByRoomType.PrivateRoom -> pv,AveragePriceByRoomType.EntireHomeApt -> ha)
println(aggData)
  connection.close();
  tabel.close()
  aggData

}


  def getAverageAnalysisOfPriceByNoOfRooms (place : String) ={
    val columnFamily:Array[Byte] = ListingsAnalysisByPlace.AveragePriceByNoOfRooms.getBytes
    val one:Array[Byte] = AveragePriceByNoOfRooms.one.getBytes
    val two:Array[Byte]= AveragePriceByNoOfRooms.two.getBytes
    val three:Array[Byte] = AveragePriceByNoOfRooms.three.getBytes
    val fourPlus:Array[Byte] = AveragePriceByNoOfRooms.fourPlus.getBytes

    //Get the Connection
    val connection = hBase.getConnection
    //Get the Table
    val tabel =  connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))
    import org.apache.hadoop.hbase.util.Bytes
    val get = new Get(Bytes.toBytes(place))
    //get.getFamilyMap;
    get.addFamily(columnFamily)

    val result = tabel.get(get)

    val oneAveragePrice:Array[Byte] = result.getValue(columnFamily,one)
    val twoAveragePrice:Array[Byte] = result.getValue(columnFamily,two)
    val threeAveragePrice:Array[Byte] = result.getValue(columnFamily,three)
    val fourPlusAveragePrice:Array[Byte] = result.getValue(columnFamily,fourPlus)
    val onePrice = Bytes.toDouble(oneAveragePrice)
    val twoPrice = Bytes.toDouble(twoAveragePrice)
    val threePrice = Bytes.toDouble(threeAveragePrice)
    val fourPrice = Bytes.toDouble(fourPlusAveragePrice)

    val aggData =Map(AveragePriceByNoOfRooms.one -> onePrice,AveragePriceByNoOfRooms.two -> twoPrice,AveragePriceByNoOfRooms.three -> threePrice,AveragePriceByNoOfRooms.fourPlus -> fourPrice)
    connection.close()
    tabel.close
    aggData
  }



  //Should be done in better way
  def getTop10ListingsByReviews(place :String) = {

    val columnFamily:Array[Byte] = ListingsAnalysisByPlace.top10ListingsReviewsByReviews.getBytes
    val first:Array[Byte] = Top10ListingsByReviews.first.getBytes()
    val second:Array[Byte]= Top10ListingsByReviews.second.getBytes()
    val third:Array[Byte] = Top10ListingsByReviews.third.getBytes()
    val fourth:Array[Byte] =Top10ListingsByReviews.fourth.getBytes
    val fifth  = Top10ListingsByReviews.fifth.getBytes()
    val sixth = Top10ListingsByReviews.sixth.getBytes()
    val seventh = Top10ListingsByReviews.seventh.getBytes()
    val eigth = Top10ListingsByReviews.eigth.getBytes()
    val ninth = Top10ListingsByReviews.ninth.getBytes()
    val tenth = Top10ListingsByReviews.tenth.getBytes()

    //Get the Connection
    val connection = hBase.getConnection
    //Get the Table
    val tabel =  connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))

    val get = new Get(Bytes.toBytes(place))
    //get.getFamilyMap;
    get.addFamily(columnFamily)

    val result = tabel.get(get)

    val resultSet:util.NavigableMap[Array[Byte],Array[Byte]]= result.getFamilyMap(columnFamily);
    println("Top 10 Result Set Size" +resultSet.size())
    val keySet:util.Set[Array[Byte]] = resultSet.keySet();
    println("Top 10 Key  Set Size" + keySet.size())

    var tableName:String = ""
    if (place.equals("Chicago")) tableName = hBaseTableNames.ListingsChicago
    else tableName = hBaseTableNames.ListingsNewyork

    println(tableName)
    val litingstable =  connection.getTable(TableName.valueOf(tableName))
    val listingDescColumnFamily  = ListingsTable.descriptionColumnName.getBytes()
    val reviewsDescCoulmnFamily = ListingsTable.reviewsColumnName.getBytes()

      //Iterate through the top10 listings to get Litings Table

    var mainList = List[Map[String,String]]()
    val top10Iterator = keySet.iterator()
     while(top10Iterator.hasNext){
       val nextListing = top10Iterator.next()
       var newMapForEachListings = scala.collection.immutable.Map[String,String]()
       println(Bytes.toInt(result.getValue(columnFamily,nextListing)))

       val getListingsDetails = new Get(result.getValue(columnFamily,nextListing))
       get.addFamily(listingDescColumnFamily);
       get.addFamily(reviewsDescCoulmnFamily);
       val litingsResult  = litingstable.get(getListingsDetails);

       val resultSetOfListingsDetails:util.NavigableMap[Array[Byte],Array[Byte]]= litingsResult.getFamilyMap(listingDescColumnFamily);
       val resultSetOfLitingsReviewDetails:util.NavigableMap[Array[Byte],Array[Byte]]= litingsResult.getFamilyMap(reviewsDescCoulmnFamily);


        val listingskeySet:util.Set[Array[Byte]] = resultSetOfListingsDetails.keySet()
         println("Size of lis" +listingskeySet.size())
        val linstingsDetailsIterator = listingskeySet.iterator()
        while(linstingsDetailsIterator.hasNext){
          var nextListingQualifier  = linstingsDetailsIterator.next()
          var key  = nextListingQualifier
          var value = litingsResult.getValue(listingDescColumnFamily,nextListingQualifier)
          newMapForEachListings = newMapForEachListings +  (Bytes.toString(key) -> Bytes.toString(value))
          println(newMapForEachListings)

        }

        val reviewkeySet:util.Set[Array[Byte]]  = resultSetOfLitingsReviewDetails.keySet()
        val reviewSetIterator = reviewkeySet.iterator()
       println("Size of rew" +reviewkeySet.size())
        while(reviewSetIterator.hasNext){
          val nextReviewQualifier  = reviewSetIterator.next()
          val key  = nextReviewQualifier
          val value = litingsResult.getValue(reviewsDescCoulmnFamily,nextReviewQualifier)

          newMapForEachListings = newMapForEachListings + (Bytes.toString(key) -> Bytes.toDouble(value).toString)
        }
        mainList = mainList :+ newMapForEachListings
       //println(mainList)
     }
    println(mainList)
    mainList
  }


}
