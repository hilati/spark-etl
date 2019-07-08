package org.lansrod.spark.etl.output.hbase


import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{Dataset, Row}
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.core.GenericType
import org.lansrod.spark.etl.output.OutputBatch
import org.lansrod.spark.etl.utils.HBaseUtils


class HBaseOutput(config: Configuration) extends OutputBatch {

  private val metaDataColumnFamily: String = config.getOpt[String](HBaseOutputConfiguration.META).getOrElse("d")
  private val table = config.getOrException[String](HBaseOutputConfiguration.TABLE)
  val getConnection: () => Connection = () => {
    ConnectionFactory.createConnection(HBaseUtils.generateDefaultConfig())
  }

  config.isDeprecated(HBaseOutputConfiguration.MAPPER, getClass.getName)



  override def saveDS(dataset: Dataset[GenericType]) : Unit = {
    try {
      import dataset.sparkSession.implicits._
      dataset.toDF().map(convert).rdd.foreachPartition(saveStream)
    } catch {
      case e: Exception =>
        print("hbase output", e)
        throw e
    }
  }

  def saveStream(partition: Iterator[(ImmutableBytesWritable, Put)]): Unit = {
    if (partition.nonEmpty) {
      var connection: Connection = null
      var bufferedMutator: BufferedMutator = null
      try {
        connection = getConnection()
        val bufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf(table))
        // we set the listener to null so that the buffered will simply throw in case an exception is thrown
        // we can also set a listener which will append to an error buffer
        bufferedMutatorParams.listener(null)
        bufferedMutator = connection.getBufferedMutator(bufferedMutatorParams)
        partition.foreach { p =>
          bufferedMutator.mutate(p._2)
        }
      }
      finally {
        if (bufferedMutator != null) {
          bufferedMutator.close()
        }
        if (connection != null) {
          connection.close()
        }
      }
    }
  }

  def convert(row: Row): (ImmutableBytesWritable, Put) = {
    val row: String = row.mkString(",").split(",")(0)
    val family = row.mkString(",").split(",")(1)
    val qualifier = row.mkString(",").split(",")(2)
    val value = row.mkString(",").split(",")(3)
    val tDateMaj = System.currentTimeMillis()
    if (row != null && !row.isEmpty) {
      val put = new Put(Bytes.toBytes(row))
      put.addColumn(family.getBytes(), qualifier.getBytes(), tDateMaj, value.getBytes())
      if (put.size() > 0) {
        put.addColumn(Bytes.toBytes(metaDataColumnFamily), Bytes.toBytes("tDateMaj"), tDateMaj, Bytes.toBytes(tDateMaj))
        return (new ImmutableBytesWritable(Bytes.toBytes(row)), put)
      } else {
        print(s"""L'identifiant du row "$row" ne peut pas être converti pour insertion dans hbase""")
      }
    }
    return null
  }


  def getJobConfiguration: org.apache.hadoop.conf.Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = new Job(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job.getConfiguration
  }
}
