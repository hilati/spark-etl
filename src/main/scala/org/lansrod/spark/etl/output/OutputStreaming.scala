package org.lansrod.spark.etl.output

import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.lansrod.spark.etl.streaming.StepStreaming

trait OutputStreaming extends StepStreaming {
  def saveStream(stream: DStream[Row]) : Unit
}
