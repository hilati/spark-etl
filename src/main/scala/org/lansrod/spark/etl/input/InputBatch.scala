package org.lansrod.spark.etl.input

import org.lansrod.spark.etl.batch.StepBatch
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}

trait InputBatch extends StepBatch {
  def createDS[rowencoder <: Product : Encoder](Ss: SparkSession):Dataset[Row]
}