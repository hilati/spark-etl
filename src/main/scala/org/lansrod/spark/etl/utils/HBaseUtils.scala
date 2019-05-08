package org.lansrod.spark.etl.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ Result, Scan}
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants}

import scala.collection.JavaConversions._
import org.apache.spark.sql.{Dataset, Row}
object HBaseUtils {



  def generateConfig(tableName: String, startrow: Option[String], families: Option[String], prefixes: Option[String], timestamp: Option[Long]): Configuration = {
    val config: Configuration = HBaseConfiguration.create()
    config.set("hbase.client.scanner.timeout.period", "900000")
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    val scan = new Scan()
    scan.setCacheBlocks(false)

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
    config.set(TableInputFormat.SCAN, convertScanToString(scan))
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

  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def generateDatasetFromHbaseTuple(hbaseTuple: (ImmutableBytesWritable, Result), dataset: Dataset[Row], allowNullValue: Boolean): Dataset[Row] = {
    val binaryRowkey = hbaseTuple._1
    val result = hbaseTuple._2
    if(result.isEmpty){
      print ("La donnée Hbase ne peut pas être parser en Dataset car elle est vide")
      return null
    }
    val sqlContext = dataset.sqlContext
    import sqlContext.implicits._
    val rowKey = Bytes.toStringBinary(binaryRowkey.get())
    for (cell: Cell <- result.listCells()) {
      val family = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
      val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
      val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      dataset.union(sqlContext.createDataset(Seq(rowKey,family,key,value)).toDF("id","family","key","value"))
    }
    dataset
  }

  /*
    def generateDatanodeFromHbaseTuple(hbaseTuple: (ImmutableBytesWritable, Result), format: Json, allowNullValue: Boolean): DataNode = {

      val binaryRowkey = hbaseTuple._1
      val result = hbaseTuple._2
      if(result.isEmpty){
        throw new TechnicalNonBlockingException("La donnée Hbase ne peut pas être parser en Datanode car elle est vide")
      }
      val dataNode = DataNodeFactoryWrapper.newNode()
      val rowKey = Bytes.toStringBinary(binaryRowkey.get())
      dataNode.setValue("id", rowKey)
      val hbaseDataNode = dataNode.newNode()
      dataNode.setValue("data", hbaseDataNode)
      for (cell: Cell <- result.listCells()) {
        val family = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
        val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)

        try {
          hbaseDataNode.newOrGetNode(family).put(key, format.decode(value, allowNullValue))
        } catch {
          case e: Exception =>
            throw new TechnicalNonBlockingException(s"Impossible de parser la donnée $rowKey - $family:$key = $value", e)
        }
      }
      dataNode
    }

    /**
      * Convert a result to a datanode. Return null if the result is empty
      * @param result - the result
      * @param format - the json format
      * @return a datanode or null if result is empty
      */
    def generateRowFromResult(result:Result, format: Json): Row = {
      if (result != null && !result.isEmpty) {
        val row = Row()
        val rowKey = Bytes.toString(result.getRow)
        dataNode.setValue("id", rowKey)
        val hbaseDataNode = dataNode.newNode()
        dataNode.setValue("data", hbaseDataNode)
        for (cell <- result.listCells()) {
          val family = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          try {
            hbaseDataNode.newOrGetNode(family).put(key, format.decode(value, false))
          } catch {
            case e: Exception =>
              throw new TechnicalNonBlockingException(s"Impossible de parser la donnée $rowKey - $family:$key = $value", e)
          }
        }
        dataNode
      }
      else {
        null
      }
    }

    def filterHbaseRecordCellsByTimestamp(hbaseRDD: RDD[(ImmutableBytesWritable, Result)], timestamp: Long): RDD[Option[(ImmutableBytesWritable, Result)]] = {
      hbaseRDD.map({ case (row, result) =>
        val cellList: List[Cell] = result.listCells().filter(cell => timestamp.equals(cell.getTimestamp)).toList
        if (cellList.nonEmpty) {
          val record = (row, Result.create(cellList))
          Option(record)
        }
        else {
          None
        }
      })
    }

    def generateGet(rowKey: String, families: Option[String], prefixes: Option[String], timestamp: Option[Long]): Get = {
      val get = new Get(Bytes.toBytes(rowKey))
      get.setCacheBlocks(false)
      families match {
        case Some(fams) if fams.nonEmpty =>
          val listFamilies = fams.split(",").toList
          listFamilies.foreach(fam => get.addFamily(Bytes.toBytes(fam)))
        case _ => get.addFamily(Bytes.toBytes("d"))
      }
      prefixes match {
        case Some(prefs) if prefs.nonEmpty =>
          val arrayPrefixes = prefs.split(",")
          val bytesPrefixesArray = arrayPrefixes.map(pref => Bytes.toBytes(pref))
          val filter = new MultipleColumnPrefixFilter(bytesPrefixesArray)
          get.setFilter(filter)
        case _ => ()
      }
      timestamp match {
        case Some(ts) => get.setTimeStamp(ts)
        case _ => ()
      }
      get
    }
  */
}