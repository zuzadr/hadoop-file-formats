package pl.edu.agh.inz.fileFormats

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WriteNYCTaxis {

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

      val taxis = spark.read.format("csv").option("delimiter",",").option("quote","").option("header", "true")
        .option("inferSchema", "true").load("./data/yellow_tripdata_2017-06.csv")

      taxis.show(5)
      val compression = args(1)

      if(args(0) == "parquet"){
        taxis.write.option("compression", compression).parquet("./data/TaxisParquet")
      }
      else if(args(0) == "orc"){
        taxis.write.option("compression", compression).orc("./data/TaxisORC")
      }
      else if(args(0) == "avro"){
        spark.conf.set("spark.sql.avro.compression.codec", compression)
        //set compression level for the deflate algorithm
        if(compression == "deflate") spark.conf.set("spark.sql.avro.deflate.level", "5")

        taxis.write.format("com.databricks.spark.avro").save("./data/TaxisAvro")
      }
      else{
        println("Please provide a valid file format and compression format")
        return
      }
      println("Writing file successful")

    }
  }

}
