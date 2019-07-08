package org.lansrod.spark.etl.output

import org.apache.spark.sql.{Dataset, Row}
import org.lansrod.spark.etl.batch.StepBatch
import org.lansrod.spark.etl.core.GenericType

trait OutputBatch extends StepBatch {
  def saveDS(dataset: Dataset[GenericType]) : Unit
}
