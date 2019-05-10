package org.lansrod.spark.etl.core

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, HBaseAdmin}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.input.Kafka.KafkaInput
import project.output.kafka.KafkaOutput
//import org.apache.spark.sql.execution.datasources.hbase._

import org.lansrod.spark.etl.input.file.Csv
import org.lansrod.spark.etl.input.hive.{HiveFactory, HiveInput}
object node extends SparkSessionCreat{
  val conf = Configuration.create(Map("sql" -> "SELECT * FROM test", "topic" ->"test"))

  import org.apache.hadoop.hbase.client.HBaseAdmin
  import org.apache.hadoop.hbase.client.HTable
  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.client.Get
  import org.apache.hadoop.hbase.util.Bytes
  import util.Properties
  import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
  import spark.implicits._


  def main(args: Array[String]): Unit = {
    val kafka = new HiveInput(conf)
    implicit val rowencoder = org.apache.spark.sql.Encoders.kryo[Row]
    var df =  Seq(
      ("010101010", "010101011110"),
      ("0101010", "01010101110"),
      ("01010110", "0101011110")
    ).toDF("key", "value")
    df = kafka.createDS(spark)
    df.show()

  }
}