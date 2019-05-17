package org.lansrod.spark.etl.output.elasticsearch

import java.util.Map.Entry

import com.google.gson.internal.LinkedTreeMap
import org.apache.spark.rdd
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.output.OutputBatch
import org.elasticsearch.spark.sql._

import scala.collection.JavaConversions._
import scala.collection.mutable

class ElasticsearchOutput(config: Configuration) extends OutputBatch {
  private val index = config.getOrException[String](ElasticsearchOutputConfiguration.INDEX)
  private val separator = config.getOpt[String](ElasticsearchOutputConfiguration.SEPARATOR).getOrElse("")
  private val attributs = config.getOpt[String](ElasticsearchOutputConfiguration.ATTRIBUTS).getOrElse("")

  protected val esConfig = Map(
    ConfigurationOptions.ES_NODES -> config.getOrException[String](ElasticsearchOutputConfiguration.HOST),
    ConfigurationOptions.ES_PORT -> config.getOrException[String](ElasticsearchOutputConfiguration.PORT)
  )


  override def saveDS(dataset: Dataset[Row]): Unit = {
    try {
      dataset.saveToEs(index)
    } catch {
      case e: Exception => print("elastic", e)
        throw e
    }
  }

}