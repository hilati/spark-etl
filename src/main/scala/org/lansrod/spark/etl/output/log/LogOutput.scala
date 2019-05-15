package org.lansrod.spark.etl.output.log

import org.apache.spark.sql.{Dataset, Row}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.output.{OutputBatch}

class LogOutput(logOutputConfiguration: Configuration) extends OutputBatch {


  override def saveDS(dataset: Dataset[Row]): Unit = {
    try {
      dataset.rdd.foreach(println)
    } catch {
      case e: Exception =>
        print("error log ",e)
        throw e
    }
  }
}
