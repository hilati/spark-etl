package org.lansrod.spark.etl.input

import org.lansrod.spark.etl.batch.StepBatch
import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait InputBatch extends StepBatch {
  def createDS(Ss: SparkSession):Dataset[Row]
}