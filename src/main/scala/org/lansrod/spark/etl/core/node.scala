package org.lansrod.spark.etl.core

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.output.elasticsearch.ElasticsearchOutput
import org.lansrod.spark.etl.output.oracle.OracleOutput
object node extends SparkSessionCreat{
  val conf = Configuration.create(Map("user" -> "spark","url"->"localhost","table"-> "test","salt"->"hilatihilatihila","password"->"","schema"->""))

  import org.apache.hadoop.hbase.client.HBaseAdmin
  import org.apache.hadoop.hbase.client.HTable
  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.client.Get
  import org.apache.hadoop.hbase.util.Bytes
  import util.Properties
  import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
  import spark.implicits._


  def main(args: Array[String]): Unit = {
    implicit val rowencoder = org.apache.spark.sql.Encoders.kryo[Row]
    val el = new OracleOutput(conf)
    var df =  Seq(
      (9,9)
    ).toDF("hwael")
    df.show()
    el.saveDS(df)
  }
}