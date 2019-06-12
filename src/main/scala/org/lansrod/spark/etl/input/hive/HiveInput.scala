package org.lansrod.spark.etl.input.hive


import org.lansrod.spark.etl.Configuration
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.lansrod.spark.etl.input.InputBatch
import org.lansrod.spark.etl.core.{GenericType, SparkSessionCreat}



class HiveInput(config: Configuration) extends InputBatch {
  private val sql: String = config.getOrException[String](HiveInputConfiguration.SQL)

  override def createDS(Ss: SparkSession):Dataset[GenericType]= {
    import Ss.implicits._
    implicit val GenericEncoder = Encoders.product[GenericType]

    try {
      val hiveContext = HiveFactory.getOrCreate(Ss.sparkContext)
      val df = hiveContext.sql(sql)
      val ds = df.as(GenericEncoder)
      ds.show()
      ds
    } catch {
      case e: Exception =>
        print("error", e)
        throw e
    }
  }

}