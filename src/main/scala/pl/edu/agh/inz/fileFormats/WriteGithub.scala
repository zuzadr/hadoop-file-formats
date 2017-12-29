package pl.edu.agh.inz.fileFormats

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WriteGithub {

  def main(args : Array[String]) : Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.length != 2) {
      println("Please provide two program arguments. \n 1. File format (parquet,orc,avro) \n 2. Compression format (gzip,lzo,snappy,deflate,zlib,none")
    }
    else {

      val spark = SparkSession
        .builder
        .appName("Github")
        .master("local[*]")
        .getOrCreate()

      val github = spark.read.format("json").json("./data/2017-06-01-8-12.json")

      github.show(5)
      val compression = args(1)

      if(args(0) == "parquet"){
        github.write.option("compression", compression).parquet("./data/GithubParquet")

      }
      else if(args(0) == "orc"){
        github.write.option("compression", compression).orc("./data/GithubORC")
      }
      else if(args(0) == "avro"){
        spark.conf.set("spark.sql.avro.compression.codec", compression)
        if(compression == "deflate") spark.conf.set("spark.sql.avro.deflate.level", "5")
        github.write.format("com.databricks.spark.avro").save("./data/GithubAvro")
      }
      else{
        println("Please provide a valid file & compression format")
        return
      }

      println("Writing file successful")

    }
  }

}
