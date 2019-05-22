package org.lansrod.spark.etl.utils

import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.Properties

import scala.collection.JavaConverters._

object File {
  def exists(path: String): Boolean = {
    Files.exists(Paths.get(path))
  }

  def getContent(path: String): Option[String] = {
    Option(exists(path)) match {
      case Some(_) => Some(scala.io.Source.fromFile(path, "utf-8").getLines().mkString("\n\r"))
      case None => None
    }
  }

  def getProperties(paths: String): Option[Map[String, String]] = {
    val prop = new Properties()

    paths.split(",").foreach { path =>
      Option(exists(path)) match {
        case Some(_) =>
          prop.load(new FileInputStream(path))
        case None => ()
      }
    }
    if (prop.isEmpty) {
      None
    } else {
      Some(prop.asScala.toMap[String, String])
    }
  }
}
