package org.lansrod.spark.etl.core

case class GenericType() {
}

object GenericType {
  def apply(id : String,family: String,key : String,value : String,version : Long): GenericType = new GenericType()
}