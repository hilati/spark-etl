package org.lansrod.spark.etl.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants}

import scala.collection.JavaConversions._
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.lansrod.spark.etl.core.GenericType
object HBaseUtils {



  def generateConfig(tableName: String, startrow: Option[String], families: Option[String], prefixes: Option[String], timestamp: Option[Long]): Configuration = {
    val config: Configuration = HBaseConfiguration.create()
    config.set("hbase.client.scanner.timeout.period", "900000")
    config.set(TableInputFormat.INPUT_TABLE, tableName)
   // val scan = new Scan()
    //scan.setCacheBlocks(false)
/*
    startrow match {
      case Some(startRow) if startRow.nonEmpty => scan.setStartRow(Bytes.toBytes(startRow))
      case _ => ()
    }
    families match {
      case Some(fams) if fams.nonEmpty =>
        val listFamilies = fams.split(",").toList
        listFamilies.foreach(fam => scan.addFamily(Bytes.toBytes(fam)))
      case _ => scan.addFamily(Bytes.toBytes("d"))
    }
    prefixes match {
      case Some(prefs) if prefs.nonEmpty =>
        val arrayPrefixes = prefs.split(",")
        val bytesPrefixesArray = arrayPrefixes.map(pref => Bytes.toBytes(pref))
        val filter = new MultipleColumnPrefixFilter(bytesPrefixesArray)
        scan.setFilter(filter)
      case _ => ()
    }
    timestamp match {
      case Some(ts) => scan.setTimeStamp(ts)
      case _ => ()
    }
    config.set(TableInputFormat.SCAN, convertScanToString(scan))*/
    config
  }

  def generateDefaultConfig(): Configuration = {
    val hbaseConf = HBaseConfiguration.create()

    /**
      * Parameters to calculate time to exception:
      *
      * ZK session timeout (zookeeper.session.timeout)
      * RPC timeout (hbase.rpc.timeout)
      * RecoverableZookeeper retry count and retry wait (zookeeper.recovery.retry, zookeeper.recovery.retry.intervalmill)
      * Client retry count and wait (hbase.client.retries.number, hbase.client.pause)
      */
    hbaseConf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, 10000L)
    hbaseConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6)
    hbaseConf.setLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE)
    hbaseConf
  }
/*
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
*/
  def generateDatasetFromHbaseTuple(hbaseTuple: (ImmutableBytesWritable, Result), dataset: Dataset[GenericType], allowNullValue: Boolean): Dataset[GenericType] = {
    val binaryRowkey = hbaseTuple._1
    val result = hbaseTuple._2
    if(result.isEmpty){
      print ("La donnée Hbase ne peut pas être parser en Dataset car elle est vide")
      return null
    }
    val sqlContext = dataset.sqlContext
    import sqlContext.sparkSession.implicits._
    implicit val GenericEncoder = Encoders.product[GenericType]

    val rowKey = Bytes.toStringBinary(binaryRowkey.get())
    for (cell: Cell <- result.listCells()) {
      val family = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
      val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
      val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      val timestamp= cell.getTimestamp()
      //case class row(id : String,family: String,key : String,value : String,version : Long)
      val tmpdataset = Seq(GenericType(rowKey,family,key,value,timestamp)).toDS()
      dataset.union(tmpdataset)
    }
    dataset
  }
}