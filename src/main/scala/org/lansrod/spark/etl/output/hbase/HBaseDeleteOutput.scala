package org.lansrod.spark.etl.output.hbase

import java.util.Map.Entry

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

class HBaseDeleteOutput(config: Configuration) extends OutputBatch with OutputStreaming {

  private val table = config.getOrException[String](HBaseOutputConfiguration.TABLE)
  private implicit val reject = rejectDatanode(config)

  override def saveStream(stream: DStream[DataNode]): Unit = {
    stream.foreachRDD(saveRDD _)
  }

  override def saveRDD(rdd: RDD[DataNode]): Unit = {
    try {
      rdd.tryMap(convert).saveAsNewAPIHadoopDataset(getJobConfiguration)
    } catch {
      case e: Exception =>
        SAMLogger.error(Brick.HBASE, e)
        throw e
    }
  }

  def convert(dataNode: DataNode): (ImmutableBytesWritable, Delete) = {
    val row: String = dataNode.getValue("id", classOf[String])
    val timestamp: Option[Long] = Option(dataNode.getValue("version", classOf[java.lang.Long])).asInstanceOf[Option[Long]]
    val data = dataNode.get("data")
    if (row != null && !row.isEmpty) {
      val delete = new Delete(Bytes.toBytes(row))

      data match {
        case node: DataNode => addDataNodetoDelete(node, timestamp, delete)
        case nodes: List[DataNode]@unchecked => nodes.foreach(addDataNodetoDelete(_, timestamp, delete))
        case nodes: java.util.List[DataNode]@unchecked => nodes.foreach(addDataNodetoDelete(_, timestamp, delete))
      }

      if (delete.size() > 0) {
        (new ImmutableBytesWritable(Bytes.toBytes(row)), delete)
      } else {
        throw new TechnicalNonBlockingException(s"""L'identifiant du datanode "$row" ne peut pas être converti pour insertion dans hbase""")
      }
    } else {
      throw new FunctionalException("L'identifiant du datanode ou les données du datanode sont vides")
    }
  }

  def addDataNodetoDelete(dataNode: DataNode, timestamp: Option[Long], delete: Delete): Unit = {
    for (d: Entry[String, Object] <- dataNode.entrySet()) {
      val key: String = d.getKey
      val t: Array[String] = key.split(":")
      if (t.length == 1) {
        timestamp match {
          case Some(time: Long) => delete.addFamily(Bytes.toBytes(t(0)), time)
          case _ => delete.addFamily(Bytes.toBytes(t(0)))
        }
      } else if (t.length == 2) {
        timestamp match {
          case Some(time: Long) => delete.addColumn(Bytes.toBytes(t(0)), Bytes.toBytes(t(1)), time)
          case _ => delete.addColumn(Bytes.toBytes(t(0)), Bytes.toBytes(t(1)))
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
}