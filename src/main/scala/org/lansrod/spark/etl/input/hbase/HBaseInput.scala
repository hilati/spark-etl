package org.lansrod.spark.etl.input.hbase

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.lansrod.spark.etl.utils.HBaseUtils
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.input.InputBatch


class HBaseInput(config: Configuration) extends InputBatch {

  private val tableName: String = config.getOrException[String](HBaseInputConfiguration.TABLE)
  private val allowNullValue: Boolean = config.getOpt[Boolean](HBaseInputConfiguration.ALLOW_NULL_VALUE).getOrElse(true)
  private val startrow: Option[String] = config.getOpt[String](HBaseInputConfiguration.STARTROW)
  private val families: Option[String] = config.getOpt[String](HBaseInputConfiguration.FAMILIES)
  private val prefixes: Option[String] = config.getOpt[String](HBaseInputConfiguration.PREFIXES)
  private val timestamp: Option[Long] = config.get(HBaseInputConfiguration.TIMESTAMP) match {
    case Some(n) => Option(n.asInstanceOf[Number].longValue())
    case _ => None
  }


  override def createDS(Ss: SparkSession): Dataset[Row]= {
    implicit val rowencoder = org.apache.spark.sql.Encoders.kryo[Row]
    val dataset = Ss.emptyDataset[Row]
    try {
          val conf: org.apache.hadoop.conf.Configuration = HBaseUtils.generateConfig(tableName, startrow, families, prefixes, timestamp)
          val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = Ss.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

          hBaseRDD.foreach( hbaseTuple => HBaseUtils.generateDatasetFromHbaseTuple(hbaseTuple, dataset, allowNullValue))
      dataset
    } catch {
      case e: Exception =>
        print(e)
        throw e
    }
  }

}
