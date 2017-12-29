
name := "Inz"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion ,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion
)

libraryDependencies += "log4j" % "log4j" % "1.2.14"

// https://mvnrepository.com/artifact/com.databricks/spark-avro
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
        