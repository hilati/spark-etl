package org.lansrod.spark.etl.input.hive

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

object HiveFactory{
  private val instances: mutable.Map[SparkContext, HiveContext] = mutable.Map()

  def getOrCreate(sparkContext: SparkContext): HiveContext = {
    try {
      instances.getOrElse(sparkContext, {
        val hiveContext = new HiveContext(sparkContext)
        val hiveConf = new HiveConf()

        hiveContext.setConf(hiveConf.getAllProperties)
        instances.put(sparkContext, hiveContext)
        hiveContext
      })
    } catch {
      case e: Exception =>
        print( "hive error",e)
        throw e
    }
  }
}
