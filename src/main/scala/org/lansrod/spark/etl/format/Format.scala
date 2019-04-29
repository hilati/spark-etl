package org.lansrod.spark.etl.format

trait Format extends Serializable {
  def decode(content:String, allowNullValue: Boolean = true) : Any
  def encode(content:Any) : String
}