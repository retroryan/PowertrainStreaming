
name := """streaming-vehicle-app"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.5"

val kafkaVersion = "0.10.0.0"
val sparkVersion = "1.6.0"
val sparkCassandraConnectorVersion = "1.6.0-M2"


libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % sparkCassandraConnectorVersion % "provided",
  "org.apache.spark"  %% "spark-mllib"           % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-graphx"          % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-sql"             % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-streaming"       % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-streaming-kafka" % sparkVersion % "provided",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2" % "provided",
  "com.datastax.cassandra" % "dse-driver" % "1.0.0" % "provided",
  "com.typesafe" % "config" % "1.3.0" % "provided"
//  "org.scala-lang" % "scala-reflect" % "2.10.5"
)
//libraryDependencies ++= Seq(
//  "com.datastax.spark" % "spark-cassandra-connector_2.10" % sparkCassandraConnectorVersion,
//  "org.apache.spark"  %% "spark-mllib"           % sparkVersion,
//  "org.apache.spark"  %% "spark-graphx"          % sparkVersion,
//  "org.apache.spark"  %% "spark-sql"             % sparkVersion,
//  "org.apache.spark"  %% "spark-streaming"       % sparkVersion,
//  "org.apache.spark"  %% "spark-streaming-kafka" % sparkVersion,
//  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2",
//  "com.datastax.cassandra" % "dse-driver" % "1.0.0",
//  "com.typesafe" % "config" % "1.3.0"
////  "org.scala-lang" % "scala-reflect" % "2.10.5"
//)