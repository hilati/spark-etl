package org.lansrod.spark.etl.output.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{Dataset, Row}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.output.OutputBatch


class HBaseDeleteOutput(config: Configuration) extends OutputBatch {

  private val table = config.getOrException[String](HBaseOutputConfiguration.TABLE)

  override def saveDS(dataset: Dataset[Row]): Unit = {
    import dataset.sparkSession.implicits._
    try {
      dataset.map(u =>convert(u)).rdd.saveAsNewAPIHadoopDataset(getJobConfiguration)
    } catch {
      case e: Exception =>
        print("HBASE output", e)
        throw e
    }
  }

  def convert(row: Row): (ImmutableBytesWritable, Delete) = {
    val row: String = row.mkString(",").split(",")(0)
    val family = row.mkString(",").split(",")(1)
    val qualifier = row.mkString(",").split(",")(2)
    val value = row.mkString(",").split(",")(3)
    val timestamp = row.mkString(",").split(",")(4).asInstanceOf[Long]

    if (row != null && !row.isEmpty) {
      val delete = new Delete(Bytes.toBytes(row))
      if (delete.size() > 0) {
        delete.addFamily(Bytes.toBytes(family),timestamp)
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),timestamp)
        return (new ImmutableBytesWritable(Bytes.toBytes(row)), delete)
      } else {
        print(s"""L'identifiant du row "$row" ne peut pas être converti pour insertion dans hbase""")
      }
    }
    return null
  }


  def getJobConfiguration: org.apache.hadoop.conf.Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = new Job(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job.getConfiguration
  }
}