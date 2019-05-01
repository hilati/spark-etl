package org.lansrod.spark.etl.core

import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.input.file.Csv
import org.lansrod.spark.etl.input.hive.{HiveFactory, HiveInput}

object node extends SparkSessionCreat{
  val conf = Configuration.create(Map("sql" -> "SELECT * FROM Persons"))


  def main(args: Array[String]): Unit = {


  }
}