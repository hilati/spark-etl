package org.lansrod.spark.etl.core

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.input.Kafka.KafkaInput
import org.lansrod.spark.etl.input.file.{Csv, MultipleCsv}
import org.lansrod.spark.etl.input.hbase.HBaseInput
import org.lansrod.spark.etl.input.hive.HiveInput
object node extends SparkSessionCreat{
  //pour tester kafka il faut lancer le broker
  val conf = Configuration.create(Map("file"->"test","broker"->"localhost:9092","topic"->"test","folder"->"/home/wael/workspace/spark-etl/test" ,"sql"->"SELECT * FROM employee","user" -> "spark","url"->"localhost","table"-> "test","salt"->"k","password"->"","schema"->""))

  import org.apache.hadoop.hbase.client.HBaseAdmin
  import org.apache.hadoop.hbase.client.HTable
  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.client.Get
  import org.apache.hadoop.hbase.util.Bytes
  import util.Properties
  import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
  import spark.implicits._


  def main(args: Array[String]): Unit = {
    val input = new KafkaInput(conf)
    /*val sqlContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)
    sqlContext.sql("CREATE TABLE IF NOT EXISTS employee(id FLOAT, name STRING, age INT, Ojja STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("INSERT INTO employee VALUES(2.2, 'Armand', 24,'belmergaz'),(2, 'Hebert', 36, 'belfruitsdemer'),(3, 'Ribeiro', 27,'belfruitsdemer'),(5, 'Savary', 58,'belfruitsdemer')")
*/

    var df =  Seq(
      (9,9)
    ).toDF("hwael","r")
    df.show()
    val ds = input.createDS(spark)
    ds.show()
    ds.printSchema()
  }
}