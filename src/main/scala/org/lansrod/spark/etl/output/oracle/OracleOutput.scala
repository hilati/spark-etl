package org.lansrod.spark.etl.output.oracle

import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Dataset, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkContext, SparkEnv}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.output.OutputBatch
import org.lansrod.spark.etl.utils.{File, Security}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

class OracleOutput(config: Configuration) extends OutputBatch {

  private lazy val logger = LoggerFactory.getLogger(getClass)
  /**
    * Setting the conf
    */
  private val driver = config.getOpt[String](OracleOutputConfiguration.DRIVER).getOrElse("oracle.jdbc.driver.OracleDriver")
  private val url = config.getOrException[String](OracleOutputConfiguration.URL)
  private val poolSize = config.getOpt[String](OracleOutputConfiguration.POOLSIZE)
  private val user = config.getOrException[String](OracleOutputConfiguration.USER)
  private val table = config.getOrException[String](OracleOutputConfiguration.TABLE)
  /** Setting salt via either file or properties **/
  private val salt = {
    config.getOpt[String](OracleOutputConfiguration.SALT_FILE) match {
      case Some(file: String) => File.getContent(file) match {
        case Some(content: String) => content
        case None => throw new RuntimeException(s"Impossible de récupérer le contenu de $file")
      }
      case _ => config.getOrException[String](OracleOutputConfiguration.SALT)
    }
  }
  private val password = Security.uncryptWithPadding(config.getOrException[String](OracleOutputConfiguration.PASSWORD), salt)
  /** Initialising the properties object used by jdbc **/
  private val connectionProperties = new Properties
  connectionProperties.setProperty("user", user)
  connectionProperties.setProperty("password", password)
  connectionProperties.setProperty("driver", driver)

  /** Parsing the Schema Lines with ; as a separator **/
  val schemaLines = config.getOrException[String](OracleOutputConfiguration.SCHEMA).split(';')

  /** Map used to parse a class name into a Data Type **/
  val parseDataTypeMap: Map[String, DataType] =
    HashMap(
      "String" -> StringType,
      "Double" -> DoubleType,
      "Integer" -> IntegerType,
      "Date" -> DateType,
      "Boolean" -> BooleanType,
      "Datetime" -> TimestampType
    )

  /** Initialising the StructType used to represent the oracle table's schema **/
  val structType : StructType =  loadSchema()


  override def saveDS(dataset: Dataset[Row]): Unit = {
    /** Initialising sql context **/
    val sc : SparkContext = dataset.sparkSession.sparkContext
    val sqlContext : SQLContext = new SQLContext(sc)


    /** Writing DF in Oracle Database **/
    val poolSizeLinkedToSparkContext = poolSize.getOrElse(sc.getConf.getInt("spark.executor.instances",6)).asInstanceOf[Int]

    dataset.coalesce(poolSizeLinkedToSparkContext)
      .selectExpr(structType.fieldNames:_*)
      .write.mode(SaveMode.Append)
      .jdbc(url, table, connectionProperties)

  }


  /**
    * Loads the table's schema found in properties into a StructType Structure
    * @return StructType containing the schema
    */
  def loadSchema(): StructType = {

    val schemaList = schemaLines.map(x=>x.split(',')).map(u => StructField(u(0),
      parseDataTypeMap.getOrElse(u(1),StringType),
      u(2).toBoolean))

    // Loading the StructFields into the StructType and returns it
    StructType(schemaList)
  }


  def getClientId(applicationId: String): java.util.function.Function[AnyRef, String] = {
    new java.util.function.Function[AnyRef, String]() {
      override def apply(t: AnyRef): String = {
        s"$applicationId-${java.net.InetAddress.getLocalHost.getHostName}-${SparkEnv.get.executorId}"
      }
    }
  }
}