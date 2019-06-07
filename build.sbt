name := "spark-etl"
version := "0.1"
scalaVersion := "2.12.8"

val sparkVersion = "2.4.2"
val kafkaVersion = "2.1.0"
val hbaseVersion = "3.0.0-SNAPSHOT"

// spark modules
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.hbase" % "hbase-spark" % "3.0.0-SNAPSHOT",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.1",
  "org.elasticsearch" 		% "elasticsearch"           % "1.7.6",
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.2",// probleme with google methode, exclude google guava is requierd
)

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.10" % "6.2.2"

//hbase module
libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-server" % "1.2.1",
  "org.apache.hbase" % "hbase-client" % "1.2.1",
  "org.apache.hbase" % "hbase-common" % "1.2.1",
)

//libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11" % "provided"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-api-scala_2.12" % "11.0",
  "org.apache.logging.log4j" % "log4j-scala" % "11.0",
)

//hadoop module
libraryDependencies ++= Seq(
  "org.apache.hadoop" %  "hadoop-core"        % "0.20.2"      % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.1.0",

  //"org.apache.hadoop" %  "hadoop-client"   % "2.7.3"% "provided",
)

libraryDependencies += "org.apache.hive"   %  "hive-exec"          % "0.8.1"       % "provided"


libraryDependencies += "net.liftweb"       %% "lift-webkit" % "3.3.0" % "compile"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
//Google module
dependencyOverrides ++= Seq(
  "com.google.collections" % "google-collections" % "1.0-rc1",
  "com.google.firebase" % "firebase-admin" % "5.9.0",
  "com.google.guava" % "guava" % "12.0",
)
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.8"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.8"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.8"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
excludeDependencies ++= Seq(
  // commons-logging is replaced by jcl-over-slf4j
  ExclusionRule("commons-logging", "commons-logging"),
  excludeJpountz
)
/*

// spark modules
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.1",
  "org.apache.hbase" % "hbase-spark" % "3.0.0-SNAPSHOT",

)

//hbase module
libraryDependencies ++= Seq(

  "org.apache.hbase" % "hbase-protocol"  % "1.2.0-cdh5.8.0",
  "org.apache.hbase" % "hbase" % "1.2.0-cdh5.8.0",
  "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.8.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.8.0",
  "org.apache.hbase" % "hbase" % "1.2.0-cdh5.8.0",
  "org.apache.hbase" % "hbase-client" % "1.2.6-cdh5.8.0",
  "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.8.0",
  "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.8.0"
)


//hadoop module
libraryDependencies ++= Seq(
  "org.apache.hadoop" %  "hadoop-core"        % "1.1.2"      % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0",
)
resolvers ++=Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Thrift" at "http://people.apache.org/~rawson/repo/",
  "CDH4" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Cloudera repo" at "https://mvnrepository.com/artifact/org.apache.kafka/kafka"
)

*/