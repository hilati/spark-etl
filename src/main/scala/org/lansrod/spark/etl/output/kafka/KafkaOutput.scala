package org.lansrod.spark.etl.output.kafka

import org.apache.spark.sql.{Dataset, Row}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.output.OutputBatch

class KafkaOutput(config : Configuration) extends OutputBatch {
  private val brokers = config.getOrException[String](KafkaOutputConfiguration.BROKER)
  private val topic = config.getOrException[String](KafkaOutputConfiguration.TOPIC)

  def saveStream(dataset: Dataset[Row]) : Unit = {
    dataset.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic",topic)
      .start()
     }
  override def saveDS(dataset : Dataset[Row]) : Unit = {
    dataset.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic",topic)
      .save()

  }
}
