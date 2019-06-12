package org.lansrod.spark.etl.input

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.lansrod.spark.etl.core.GenericType
import org.lansrod.spark.etl.streaming.StepStreaming

trait InputStreaming extends StepStreaming {
  def createStream(ssc:StreamingContext) : DStream[Dataset[GenericType]]
}