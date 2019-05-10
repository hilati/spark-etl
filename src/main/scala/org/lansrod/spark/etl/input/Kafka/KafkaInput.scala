package org.lansrod.spark.etl.input.Kafka


import kafka.api.OffsetRequest
import kafka.serializer.StringDecoder
import org.apache.avro.data.Json
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.lansrod.spark.etl.Configuration
import org.lansrod.spark.etl.format.Format
import org.lansrod.spark.etl.input.InputBatch

class KafkaInput(config:Configuration) extends InputBatch {


	private val broker = config.getOrException[String](KafkaInputConfiguration.BROKER)
	private val topic = config.getOrException[String](KafkaInputConfiguration.TOPIC)
	private val resetOpt = config.get(KafkaInputConfiguration.RESET)

	/*
	private lazy val format = config.getOpt[String](KafkaInputConfiguration.FORMAT) match {
		case Some(formatClassName) =>
			val constructor = Class.forName(formatClassName).getConstructor()
			constructor.newInstance().asInstanceOf[Format]
		case None => new Json()
	}
	override def createStream(ssc:StreamingContext) : DStream[Dataset[Row]]={
		try {
			val topics = Set(topic)
			var topicParameters = Map(
				"bootstrap.servers" -> broker
			)
			resetOpt match {
				case Some("true") => topicParameters += "auto.offset.reset" -> "smallest"
				case _ => ()
			}

			KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, topicParameters, topics)
				.map{ _._2 }
				.mapPartitions( parsePartition )
		} catch {
			case e: Exception =>
				print("strim kafka",e)
				throw e
		}
	}
	def parsePartition(partition:Iterator[String]) : Iterator[Row] = {
		try {
			partition.flatMap {
				case (content: String) =>
					try {
						format.decode(content) match {
							case x: DataNode => List(x)
							case _ => throw new RuntimeException(s"Le contenu n'est pas au format objet JSON")
						}
						List()
					} catch {
						case e: Throwable => List()
					}
				case _ => List()
			}
		} catch {
			case e: Exception =>
				SAMLogger.error(Brick.KAFKA,e)
				throw e
		}
	}
*/

	def createStream(Ss : SparkSession) : Dataset[Row] = {
		try {
			var df = Ss
			.readStream
			.format ("kafka")
			.option ("kafka.bootstrap.servers", broker)
			.option ("subscribe", topic)
			.option ("startingOffsets", OffsetRequest.EarliestTime)
			.load ()
			df
		} catch {
		case e: Exception =>
		print ("kafka", e)
		throw e
		}
	}
	override def createDS(Ss: SparkSession): Dataset[Row] = {
		try {
			val df = Ss
			.read
			.format("kafka")
			.option("kafka.bootstrap.servers", broker)
			.option("subscribe", topic)
			.option("startingOffsets",  OffsetRequest.EarliestTime)
			.load()
			df
		} catch {
			case e: Exception =>
				print("kafka", e)
				throw e
		}
	}

}