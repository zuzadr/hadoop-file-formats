package pl.edu.agh.inz.fileFormats


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadNYCTaxis{


  def main(args : Array[String]) : Unit = {

    if(args.length == 0 ){
      println("Please select the file format you wish to read as a program argument")
      println("Possible formats: avro, orc, parquet, csv")
    }
    else {
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .getOrCreate()

      var df :DataFrame = null
      if(args(0) == "avro"){
        val startTime = System.nanoTime()
        df = spark.read.format("com.databricks.spark.avro").load("./data/TaxisAvro")
        val endTime = System.nanoTime()
        println("Reading took " + (endTime - startTime) + " ns")

      }
      else if(args(0) == "parquet"){
        val startTime = System.nanoTime()
        df = spark.read.parquet("./data/TaxisParquet")
        val endTime = System.nanoTime()
        println("Reading took " + (endTime - startTime) + " ns")
      }
      else if(args(0) == "orc"){
        val startTime = System.nanoTime()
        df = spark.read.orc("./data/TaxisORC")
        val endTime = System.nanoTime()
        println("Reading took " + (endTime - startTime) + " ns")
      }
      else if(args(0) == "csv"){
        val startTime = System.nanoTime()
        df = spark.read.format("csv").option("delimiter",",").option("quote","").option("header", "true")
          .option("inferSchema", "true").load("./data/yellow_tripdata_2017-06.csv")
        val endTime = System.nanoTime()
        println("Reading took " + (endTime - startTime) + " ns")

      }
      else{
        println("Please provide a proper format name")
        return
      }

      df.show(10)
      df.printSchema()
    }
  }

}


