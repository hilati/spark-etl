package org.lansrod.spark.etl.core

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.input.Kafka.KafkaInput
import org.lansrod.spark.etl.input.file.{Csv, MultipleCsv}
import org.lansrod.spark.etl.input.hbase.HBaseInput
import org.lansrod.spark.etl.input.hive.HiveInput
import org.lansrod.spark.etl.output.elasticsearch.ElasticsearchOutput
import org.lansrod.spark.etl.output.kafka.KafkaOutput
import org.lansrod.spark.etl.output.log.LogOutput
import org.lansrod.spark.etl.output.oracle.OracleOutput
import org.elasticsearch.spark.sql._
import org.lansrod.spark.etl.output.hbase.HBaseOutput

object node extends SparkSessionCreat{
  //pour tester kafka il faut lancer le broker
  val conf = Configuration.create(Map("index"-> "index/type","hostname"-> "localhost","port"->"9200","file"->"test","broker"->"localhost:9092","topic"->"test","folder"->"/home/wael/workspace/spark-etl/test" ,"sql"->"SELECT * FROM employee","user" -> "spark","url"->"localhost","table"-> "test","salt"->"","password"->"","schema"->"hi"))

  import org.apache.hadoop.hbase.client.HBaseAdmin
  import org.apache.hadoop.hbase.client.HTable
  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.client.Get
  import org.apache.hadoop.hbase.util.Bytes
  import util.Properties
  import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
  import spark.implicits._


  def main(args: Array[String]): Unit = {
    val input = new HiveInput(conf)
    val output = new HBaseOutput(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)
    sqlContext.sql("CREATE TABLE IF NOT EXISTS employee(key INT, value FLOAT, name STRING, age INT, Ojja STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("INSERT INTO employee VALUES(2,2.2, 'Armand', 24,'belmergaz'),(2,2, 'Hebert', 36, 'belfruitsdemer'),(4,3, 'Ribeiro', 27,'belfruitsdemer'),(4,5, 'Savary', 58,'belfruitsdemer')")


    var df =  Seq(
      (9,9)
    ).toDF("hwael","r")
    //spark.createDataset(Seq("j")).saveToEs("wael/hi")
    val ds = input.createDS(spark)
    ds.show()
    output.saveDS(ds)
    //ds.show()
    //ds.printSchema()
  }
}