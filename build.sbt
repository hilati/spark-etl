name := "spark-etl"
version := "0.1"
scalaVersion := "2.12.8"
val sparkVersion = "2.4.2"
val kafkaVersion = "2.1.0"

resolvers ++=Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Thrift" at "http://people.apache.org/~rawson/repo/",
  "CDH4" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Cloudera repo" at "https://mvnrepository.com/artifact/org.apache.kafka/kafka"
)


// spark modules
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.1"
)

//hbase module
libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-server" % "1.2.1",
  "org.apache.hbase" % "hbase-client" % "1.2.1",
  "org.apache.hbase" % "hbase-common" % "1.2.1",
)

// log modules
libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-api-scala_2.12" % "11.0",
  "org.apache.logging.log4j" % "log4j-scala" % "11.0",
)

//hadoop module
libraryDependencies ++= Seq(
  "org.apache.hadoop" %  "hadoop-core"        % "0.20.2"      % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3"
)

libraryDependencies += "org.apache.hive"   %  "hive-exec"          % "0.8.1"       % "provided"


libraryDependencies += "net.liftweb"       %% "lift-webkit" % "3.3.0" % "compile"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
