package hBase

import javax.inject.{Inject, Singleton}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result

import scala.compat.java8.FutureConverters
import scala.concurrent.Future

/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class AverageAnalysisOfListing {



def getAverageAnalysisOfPriceByPlace(place : String):Future[Map[String,Double]] ={
  val columnFamily:Array[Byte] = ListingsAnalysisByPlace.AveragePriceByRoomType.getBytes
  val homeColumn:Array[Byte] = AveragePriceByRoomType.EntireHomeApt.getBytes
  val sharedColumn:Array[Byte]= AveragePriceByRoomType.SharedRoom.getBytes
  val privateColumn:Array[Byte] = AveragePriceByRoomType.PrivateRoom.getBytes

  //Get the Connection
  val connection =hBase.getConnection
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
  val aggData = Map(AveragePriceByRoomType.SharedRoom -> sh,AveragePriceByRoomType.PrivateRoom -> pv,AveragePriceByRoomType.EntireHomeApt -> ha)
  Future.successful(aggData)
}


}
