package org.lansrod.spark.etl.core

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.input.hive.HiveInput
import org.lansrod.spark.etl.output.elasticsearch.ElasticsearchOutput
import org.lansrod.spark.etl.output.oracle.OracleOutput
object node extends SparkSessionCreat{
  val conf = Configuration.create(Map("sql"->"SELECT * FROM employee","user" -> "spark","url"->"localhost","table"-> "test","salt"->"hilatihilatihila","password"->"","schema"->""))

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
    val el = new HiveInput(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)
    sqlContext.sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("INSERT INTO employee VALUES('Rébecca', 'Armand', 24),('Aimée', 'Hebert', 36),('Marielle', 'Ribeiro', 27),('Hilaire', 'Savary', 58)")


    var df =  Seq(
      (9,9)
    ).toDF("hwael","r")
    df.show()
    el.createDS(spark)
  }
}