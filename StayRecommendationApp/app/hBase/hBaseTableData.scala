package hBase

import javax.inject.Singleton

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by vinay on 4/23/17.
  */

@Singleton
class hBaseTableData {






  def anotherMethod = {

    //Get the Connection
    val connection = hBase.getConnection
    //Get the Table
    val table =  connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))


    //val scan = new Scan;

    val scan = new Scan;
    scan.addFamily(ListingsAnalysisByPlace.AveragePriceByRoomType.getBytes);
    scan.addFamily(ListingsAnalysisByPlace.AveragePriceByNoOfRooms.getBytes);


    import org.apache.hadoop.hbase.client.ResultScanner
    import org.apache.hadoop.hbase.util.Bytes
    import java.util
    import scala.collection.JavaConversions._
    var mainlist = List[List[String]]()



    val resultScanner = table.getScanner(scan)
    val iterator = resultScanner.iterator
    while ( {
      iterator.hasNext
    }) {
      val next = iterator.next
      var eachRowList = List[String]()
      //print(Bytes.toString(next.getRow))
      eachRowList =  eachRowList :+ Bytes.toString(next.getRow)
      //:: eachRowList
      import scala.collection.JavaConversions._
      for (columnFamilyMap <- next.getMap.entrySet) {
        for (entryVersion <- columnFamilyMap.getValue.entrySet) {
          import scala.collection.JavaConversions._
          for (entry <- entryVersion.getValue.entrySet) {
            val row = Bytes.toString(next.getRow)
            val column = Bytes.toString(entryVersion.getKey)
            val value = Bytes.toDouble(entry.getValue)
            //eachRowList =  eachRowList :: Bytes.toString(entryVersion.getKey)
            eachRowList =   eachRowList :+ Bytes.toDouble(entry.getValue).toString
            //eachRowList.++(Bytes.toString(entryVersion.getKey))
            //eachRowList.++(Bytes.toDouble(entry.getValue).toString)
          }

        }
        //println(eachRowList)
    }
       mainlist =  mainlist :+ eachRowList
      }
        mainlist.map(x => println(x))
        mainlist

    }
  }


