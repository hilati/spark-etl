package org.lansrod.spark.etl


import scala.collection.mutable

class Configuration extends mutable.HashMap[String, Any] {
  def getOrException[T <: Any](property: String): T = {
    get(property) match {
      case Some(c) => c.asInstanceOf[T]
      case _ => throw new RuntimeException(s"Le paramètre $property doit être renseigné, ${this}")
    }
  }

  def getOrException[T <: Any](properties: String*): T = {
    var result:Option[Any] = null
    properties.foreach(property => {
      val resolvedProperty = get(property)
      if (!resolvedProperty.isEmpty) {
        result = resolvedProperty
      }
    })
    return result match {
      case Some(c) => {
        c.asInstanceOf[T]
      }
      case _ => throw new RuntimeException(s"Un des paramètres $properties doit être renseigné, ${this}")
    }
  }

  def getOpt[T <: Any](property: String): Option[T] = {
    get(property) match {
      case Some(c) => Some(c.asInstanceOf[T])
      case _ => None
    }
  }


  def isDeprecated(property: String, fromClass: String): Unit = {
    get(property) match {
      case Some(c) =>
        throw new RuntimeException(s"La property $property est deprecated pour la patate $fromClass")
      case _ => ()
    }
  }
}

object Configuration {
  val CLASS = "class"
  val CONFIG = "config"
  val NEXT = "next"

  def create(map: Map[String, Any]): Configuration = {
    val config = new Configuration()
    config.++=(map)
    config
  }

  def create(map: Option[Any]): Configuration = {
    val config = new Configuration()
    map match {
      case Some(m: Map[String, Any]@unchecked) => config.++=(m)
      case Some(_) => config
      case None => config
    }
  }
}