package hBase

import javax.inject.Singleton
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Mutation
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
  * Created by vinay on 4/16/17.
  */

object hBase {


  import org.apache.hadoop.hbase.client.ConnectionFactory


  //Return Connection pool for an HBase instance

  def getConnection ={
    val conf: Configuration= HBaseConfiguration.create
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }

}
