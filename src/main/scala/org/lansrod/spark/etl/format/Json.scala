package org.lansrod.spark.etl.format

import java.util

import net.liftweb.json._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.json4s.DefaultFormats

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/*class Json extends Format {

  override def decode(content: String, allowNullValue: Boolean = true): Any = {
    implicit val formats = DefaultFormats
    val rawJson = parse(content)
    rawJson match {
      case jvalue: JValue => fromJValue(jvalue, DataNodeFactoryWrapper.newNode(), allowNullValue)
      case _ => throw new RuntimeException(s"Le contenu n'est pas au format JSON")
    }
  }

  override def encode(content: Any): String = {
    compact(render(toJValue(content)))
  }

  def fromJValue(value: JValue, row: Row, allowNullValue: Boolean): Any = {
    value match {
      case x: JArray =>
        x.arr.map(value => fromJValue(value, dataNode.newNode(), allowNullValue)).asJava
      case x: JObject =>
        x.obj.foreach(field => dataNode.put(field.name, fromJValue(field.value, dataNode.newNode(), allowNullValue)))
        dataNode
      case x: JInt => x.values.longValue()
      case x if x == JNull => if (allowNullValue) new NullValue else null
      case x: JValue => x.values
    }
  }

  private def toJValue(any: Any): JValue = {
    any match {
      case x: BigInt => JInt(x)
      case x: String => JString(x)
      case x: BigDecimal => JDouble(x.doubleValue())
      case x: java.lang.Integer => JInt(x.intValue())
      case x: java.lang.Long => JInt(x.longValue())
      case x: java.lang.Double => JDouble(x.doubleValue())
      case x: java.lang.Float => JDouble(x.floatValue())
      case x: java.lang.Boolean => JBool(x)
      // Traitement spécifique pour les objets de type Map -> transformation en JObject
      case x: Map[String, Any]@unchecked =>
        var tmp: List[JField] = List[JField]()
        x.foreach { tuple =>
          tmp = tmp.::(JField(tuple._1, toJValue(tuple._2)))
        }
        JObject(tmp)
      case x: util.HashMap[String, Any]@unchecked =>
        var tmp: List[JField] = List[JField]()
        x.foreach { tuple =>
          tmp = tmp.::(JField(tuple._1, toJValue(tuple._2)))
        }
        JObject(tmp)
      // Traitement spécifique pour les objets de type List -> transformation en JArray
      case x: List[Any]@unchecked =>
        var tmp: List[JValue] = List[JValue]()
        x.foreach { tuple =>
          tmp = tmp.::(toJValue(tuple))
        }
        JArray(tmp)
      case x: util.List[Any]@unchecked =>
        var tmp: List[JValue] = List[JValue]()
        x.foreach { tuple =>
          tmp = tmp.::(toJValue(tuple))
        }
        JArray(tmp)
      case x: Result =>
        JObject(JField("rowkey", JString(Bytes.toString(x.getRow))) :: JField("result", JString(x.toString)) :: Nil)
      case _ => JNull
    }
  }

}
*/