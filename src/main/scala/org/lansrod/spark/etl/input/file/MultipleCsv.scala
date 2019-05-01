package org.lansrod.spark.etl.input.file

import org.lansrod.spark.etl.Configuration
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.lansrod.spark.etl.input.InputBatch

class MultipleCsv(config: Configuration) extends InputBatch {

  private val folder = config.getOrException[String](FileConfiguration.FOLDER)
  private val delimiter = config.getOpt[String](CsvConfiguration.DELIMITER).getOrElse(CsvConfiguration.DEFAULT_DELIMITER)
  private val useHeader = config.getOpt[String](CsvConfiguration.USE_HERADER).getOrElse("true")
  private val FileExtension = config.getOpt[String](FileConfiguration.FILE_EXTENSION).getOrElse("csv")

  override def createDS(Ss: SparkSession): Dataset[Row] = {
    implicit val rowencoder = org.apache.spark.sql.Encoders.kryo[Row]
    try {
      val sQLContext = new SQLContext(Ss.sparkContext)
      sQLContext.read
        .format("com.databricks.spark.csv")
        .option("header", useHeader)
        .option("parserLib", "univocity")
        .option("delimiter", delimiter)
        .load(folder + "/*." + FileExtension)
        .as(rowencoder)
    } catch {
      case e: Exception =>
        print(e)
        throw e
    }
  }
}
