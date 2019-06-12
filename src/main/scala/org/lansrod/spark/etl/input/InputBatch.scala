package org.lansrod.spark.etl.input

import org.lansrod.spark.etl.batch.StepBatch
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.lansrod.spark.etl.core.GenericType

trait InputBatch extends StepBatch {
  def createDS(Ss: SparkSession):Dataset[GenericType]
}