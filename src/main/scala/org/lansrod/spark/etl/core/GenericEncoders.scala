package org.lansrod.spark.etl.core

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.Encoders.product
object GenericEncoders{

  def csv(schema : StructType): Encoder[GenericType] = {
    import Schema2CaseClass.implicits._
    import scala.reflect.runtime.universe._
    import scala.reflect.runtime.currentMirror
    //import scala.tools.reflect.ToolBox

    // TO compile and run code we will use a ToolBox api.
    //val toolbox = currentMirror.mkToolBox()
    //Schema2CaseClass.Schema2CaseClass(schema,"GenericType")
    product[GenericType]
  }





}