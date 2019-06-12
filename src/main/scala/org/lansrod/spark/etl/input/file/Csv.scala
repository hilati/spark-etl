package org.lansrod.spark.etl.input.file


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoders, Row, SQLContext, SparkSession}
import org.lansrod.spark.etl.core.{GenericEncoders, GenericType}
import org.lansrod.spark.etl.input.InputBatch
import org.lansrod.spark.etl.utils.ZipFileInputFormat

class Csv(config: org.lansrod.spark.etl.Configuration) extends InputBatch {

  val ZipExtention = ".zip"
  val DefaultCharset = "UTF-8"
  private val file = config.getOrException[String](FileConfiguration.FILE)
  private val delimiter = config.getOpt[String](CsvConfiguration.DELIMITER).getOrElse(CsvConfiguration.DEFAULT_DELIMITER)
  private val charset = config.getOpt[String](FileConfiguration.CHARSET).getOrElse(DefaultCharset)
  private val folder = config.getOpt[String](FileConfiguration.FOLDER)

  override def createDS(Ss: SparkSession): Dataset[GenericType] = {
    print( "createDS: input file: " + file)
    try {
      import Ss.implicits._

      ZipUtils.init(Ss.sparkContext)
      val csvFile = if (file.endsWith(ZipExtention)) ZipUtils.unzipAndGetPath(file) else Some(file)
      if (csvFile.isEmpty) {
        return Ss.emptyDataset[GenericType]
      }
      print( "csvFile: " + csvFile)
      implicit val GenericEncoder = Encoders.product[GenericType]

      val sQLContext = new SQLContext(Ss.sparkContext)
      val df = sQLContext.read
              .format("com.databricks.spark.csv")
              .option("header", "true")
              .option("parserLib", "univocity")
              .option("delimiter", delimiter)
              .option("charset", charset)
              .option("inferSchema", "true")
              .load(csvFile.get)

      val ds = df.as(GenericEncoder)
      ds
    } catch {
      case e: Exception =>
        print("error csv", e)
        throw e
    }
  }


  object ZipUtils extends Serializable {

    @transient var sc: SparkContext = _

    def init(context: SparkContext): Unit = {
      sc = context
    }

    def unzipAndGetPath(filePath: String): Option[String] = {

      val zipFileRow = sc.newAPIHadoopFile(
        FileSystem.get(new Configuration()).getFileStatus(new Path(filePath)).getPath.toString,
        classOf[ZipFileInputFormat],
        classOf[Text],
        classOf[BytesWritable], sc.hadoopConfiguration)
      val zipPath = zipFileRow.map {
        y => ProcessFile(y._1.toString, y._2)
      }
      if (zipPath.isEmpty()) {
        print("No file found in zip")
        return None
      }
      zipPath.first()
    }
  }

  private object ProcessFile extends Serializable {

    def apply(fileName: String, records: BytesWritable): Option[String] = {
      if (records.getLength <= 0) {
        print("ProcessFile: empty")
        return None
      }
      folder match {
        case Some(folderPath) =>
          val filePath = folderPath + "/" + fileName
          val outFileStream = FileSystem.get(new Configuration()).create(new Path(filePath), true)
          outFileStream.write(records.getBytes)
          outFileStream.close()
          Some(filePath)
        case _ => throw new RuntimeException(s"Le paramètre ${FileConfiguration.FOLDER} doit être renseigné, ${this}")
      }
    }
  }

}