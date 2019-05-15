package org.lansrod.spark.etl.output.hbase

import java.util.Map.Entry

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.streaming.dstream.DStream
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.output.OutputBatch

import scala.collection.JavaConversions._

class HBaseOutput(config: Configuration) extends OutputBatch {

  private lazy val format = new Json()
  private val metaDataColumnFamily: String = config.getOpt[String](HBaseOutputConfiguration.META).getOrElse("d")
  private val table = config.getOrException[String](HBaseOutputConfiguration.TABLE)
  private val encode = LovUtils.validateEncode(config.getOpt[String](HBaseOutputConfiguration.ENCODE).getOrElse(""))
  private val test = CustomWorkflowParameters.testMode
  private val lovCodes = LovUtils.getLovCodes
  private val lovMappings = LovUtils.getLovMappings
  private implicit val reject = rejectDatanode(config)
  val getConnection: () => Connection = () => {
    ConnectionFactory.createConnection(HBaseUtils.generateDefaultConfig())
  }

  config.isDeprecated(HBaseOutputConfiguration.MAPPER, getClass.getName)

  override def saveStream(stream: DStream[DataNode]): Unit = {
    stream.foreachRDD(saveRDD _)
  }

  override def saveDS(dataset: Dataset[Row]) : Unit = {
    try {
      import dataset.sparkSession.implicits._
      dataset.map(convert).rdd.foreachPartition(saveStream)
    } catch {
      case e: Exception =>
        print("hbase output", e)
        throw e
    }
  }

  def saveStream(partition: Iterator[(ImmutableBytesWritable, Put)]): Unit = {
    if (partition.nonEmpty) {
      var connection: Connection = null
      var bufferedMutator: BufferedMutator = null
      try {
        connection = getConnection()
        val bufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf(table))
        // we set the listener to null so that the buffered will simply throw in case an exception is thrown
        // we can also set a listener which will append to an error buffer
        bufferedMutatorParams.listener(null)
        bufferedMutator = connection.getBufferedMutator(bufferedMutatorParams)
        partition.foreach { p =>
          bufferedMutator.mutate(p._2)
        }
      }
      finally {
        if (bufferedMutator != null) {
          bufferedMutator.close()
        }
        if (connection != null) {
          connection.close()
        }
      }
    }
  }

  def convert(row: Row): (ImmutableBytesWritable, Put) = {
    val row: String = row.mkString(",").split(",")(0)
    val family = row.mkString(",").split(",")(1)
    val qualifier = row.mkString(",").split(",")(2)
    val value = row.mkString(",").split(",")(3)
    val tDateMaj = System.currentTimeMillis()
    if (row != null && !row.isEmpty) {
      val put = new Put(Bytes.toBytes(row))
      put.addColumn(family.getBytes(), qualifier.getBytes(), tDateMaj,value.getBytes())


      if (put.size() > 0) {
        /*---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
          * Quand on est en mode test, on met la valeur par defaut (9223372036854775807L) dans le timestamp. Sinon, on verifie si le champ version contient un long. Si c'est le cas, on met dans
          * le timestamp du champ tDateMaj la valeur du champs version (meme valeur que les autres champs ajoutés) sinon, on met la valeur par defaut (9223372036854775807L) dans le timestamp
          ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
        if (test) {
          put.addColumn(Bytes.toBytes(metaDataColumnFamily), Bytes.toBytes("tDateMaj"), tDateMaj, Bytes.toBytes(format.encode(tDateMaj)))
          timestamp match {
            case Some(time: Long) => put.addColumn(Bytes.toBytes(metaDataColumnFamily), Bytes.toBytes("tVersionTest_" + time + "_" + tDateMaj), tDateMaj, Bytes.toBytes(format.encode("--test")))
            case _ => put.addColumn(Bytes.toBytes(metaDataColumnFamily), Bytes.toBytes("tVersionTest_" + tDateMaj + "_" + tDateMaj), tDateMaj, Bytes.toBytes(format.encode("--test")))
          }
        } else {
          timestamp match {
            case Some(time: Long) => put.addColumn(Bytes.toBytes(metaDataColumnFamily), Bytes.toBytes("tDateMaj"), time, Bytes.toBytes(format.encode(tDateMaj)))
            case _ => put.addColumn(Bytes.toBytes(metaDataColumnFamily), Bytes.toBytes("tDateMaj"), tDateMaj, Bytes.toBytes(format.encode(tDateMaj)))
          }
        }
        (new ImmutableBytesWritable(Bytes.toBytes(row)), put)
      } else {
        throw new TechnicalNonBlockingException(s"""L'identifiant du datanode "$row" ne peut pas être converti pour insertion dans hbase""")
      }
    } else {
      throw new FunctionalException("L'identifiant du datanode ou les données du datanode sont vides")
    }
  }

  def addDataNodetoPut(dataNode: DataNode, timestamp: Option[Long], tDateMaj: Long, put: Put): Unit = {
    val mappings = lovMappings.getOrElse(Map())
    val codes = lovCodes.getOrElse(Map())
    val objectName = table.split(":").last
    for (d: Entry[String, Object] <- dataNode.entrySet()) {
      val key: String = d.getKey
      val t: Array[String] = key.split(":")
      val value = d.getValue
      val formattedAttr = formatAttr(t(1))
      timestamp match {
        case Some(time: Long) if !test =>
          value match {
            case valueIsString: String if !encode.isEmpty =>
              put.addColumn(Bytes.toBytes(t(0)), Bytes.toBytes(t(1)), time, Bytes.toBytes(format.encode(LovUtils.encode(encode, formattedAttr, valueIsString, mappings, codes))))
            case _: Any => put.addColumn(Bytes.toBytes(t(0)), Bytes.toBytes(t(1)), time, Bytes.toBytes(format.encode(d.getValue)))
          }
        case _ =>
          value match {
            case valueIsString: String if !encode.isEmpty =>
              put.addColumn(Bytes.toBytes(t(0)), Bytes.toBytes(t(1)), tDateMaj, Bytes.toBytes(format.encode(LovUtils.encode(encode, formattedAttr, valueIsString, mappings, codes))))
            case _: Any => put.addColumn(Bytes.toBytes(t(0)), Bytes.toBytes(t(1)), tDateMaj, Bytes.toBytes(format.encode(d.getValue)))
          }
      }
    }
  }

  def getJobConfiguration: org.apache.hadoop.conf.Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job.getConfiguration
  }

  private def formatAttr(fullAttr: String): String = {
    val splittedAttr = fullAttr.split("_")
    val lastIndex = splittedAttr.length - 1
    val attrName = splittedAttr(lastIndex)
    val prefixIndex = lastIndex - 1 // le prefixe est tjours sur deux caracteres ?
    if (prefixIndex > 0) {
      s"$attrName|${splittedAttr.slice(0, prefixIndex).mkString("_")}"
    }
    else {
      attrName
    }
  }
}
