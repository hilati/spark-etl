package org.lansrod.spark.etl.input

import org.lansrod.spark.etl.batch.StepBatch
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.SparkContext

trait InputBatch extends StepBatch {
  def createDS[T <: Product : Encoder](sc: SparkContext)
}