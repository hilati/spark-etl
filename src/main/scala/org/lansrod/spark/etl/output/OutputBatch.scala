package org.lansrod.spark.etl.output

import org.apache.spark.sql.{Dataset, Row}
import org.lansrod.spark.etl.batch.StepBatch

trait OutputBatch extends StepBatch {
  def saveDS(dataset: Dataset[Row]) : Unit
}
