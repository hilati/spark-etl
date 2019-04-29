package org.lansrod.spark.etl.core

import org.apache.spark.sql.SparkSession

trait SparkSessionCreat {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark")
      .getOrCreate()
  }

}