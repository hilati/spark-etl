package org.lansrod.spark.etl.input.hive


import org.lansrod.spark.etl.Configuration
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.lansrod.spark.etl.input.InputBatch
import org.lansrod.spark.etl.core.SparkSessionCreat



class HiveInput(config: Configuration) extends InputBatch {
  private val sql: String = config.getOrException[String](HiveInputConfiguration.SQL)

  override def createDS[Rowencoder : Encoder](Ss: SparkSession):Dataset[Row]= {
    import Ss.implicits._
    implicit val Rowencoder = org.apache.spark.sql.Encoders.kryo[Row]

    try {
      val hiveContext = HiveFactory.getOrCreate(Ss.sparkContext)
      val df = hiveContext.sql(sql)
      val ds = df.as(Rowencoder)
      ds
    } catch {
      case e: Exception =>
        print("error", e)
        throw e
    }
  }

}