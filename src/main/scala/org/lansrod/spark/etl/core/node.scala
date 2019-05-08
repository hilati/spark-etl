package org.lansrod.spark.etl.core

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, HBaseAdmin}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.lansrod.spark.etl.Configuration
//import org.apache.spark.sql.execution.datasources.hbase._

import org.lansrod.spark.etl.input.file.Csv
import org.lansrod.spark.etl.input.hive.{HiveFactory, HiveInput}
object node extends SparkSessionCreat{
  val conf = Configuration.create(Map("table" -> "test"))

  import org.apache.hadoop.hbase.client.HBaseAdmin
  import org.apache.hadoop.hbase.client.HTable
  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.client.Get
  import org.apache.hadoop.hbase.util.Bytes
  import util.Properties
  import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}

  def main(args: Array[String]): Unit = {
   // val hbase = new HBaseInput(conf)
    //hbase.createDS(spark)


  }
}