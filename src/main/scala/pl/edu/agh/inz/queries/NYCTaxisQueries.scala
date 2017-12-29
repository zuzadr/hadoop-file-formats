package pl.edu.agh.inz.queries


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object NYCTaxisQueries {

  def selectStar(spark : SparkSession) : Long = {
    var totalTime=0 : Long
    for(i <- 1 to 10){
      val t0 = System.nanoTime()
      spark.sql("SELECT * FROM nycTaxis")
      val t1 = System.nanoTime()
      totalTime+=(t1 - t0)
    }
    println("[select * ]Averaged time for 10 executions: "+ totalTime/10 + " ns")
    totalTime
  }

  def selectColumns(spark: SparkSession) : Long = {
    var totalTime=0 : Long
    for(i <- 1 to 10){
      val t0 = System.nanoTime()
      spark.sql("SELECT trip_distance,total_amount FROM nycTaxis")
      val t1 = System.nanoTime()
      totalTime+=(t1 - t0)
    }
    println("[select columns] Averaged time for 10 executions: "+ totalTime/10 + " ns")
    totalTime
  }

  def selectColumnsWhere(spark : SparkSession) : Long = {
    var totalTime=0 : Long
    for(i <- 1 to 10){
      val t0 = System.nanoTime()
      spark.sql("SELECT tpep_pickup_datetime,tpep_dropoff_datetime,trip_distance,total_amount FROM nycTaxis WHERE payment_type=2")
      val t1 = System.nanoTime()
      totalTime+=(t1 - t0)
    }
    println("[select some columns with where clause] Averaged time for 10 executions: "+ totalTime/10 + " ns")
    totalTime
  }

  def selectCalc(spark : SparkSession) : Long = {
    var totalTime=0 : Long
    for(i <- 1 to 10){
      val t0 = System.nanoTime()
      spark.sql("SELECT (minute(tpep_dropoff_datetime)-minute(tpep_pickup_datetime)) AS trip_duration,total_amount FROM nycTaxis")
      val t1 = System.nanoTime()
      totalTime+=(t1 - t0)
    }
    println("[select with calculations] Averaged time for 10 executions: "+ totalTime/10 + " ns")
    totalTime
  }

  def selectWithAgg(spark : SparkSession) : Long = {
    var totalTime=0 : Long
    for(i <- 1 to 10){
      val t0 = System.nanoTime()
      spark.sql("SELECT avg(tip_amount) as avg_tips, RatecodeID FROM nycTaxis GROUP BY RatecodeID")
      val t1 = System.nanoTime()
      totalTime+=(t1 - t0)
    }
    println("[select with aggregating functions] Averaged time for 10 executions: "+ totalTime/10 + " ns")
    totalTime
  }


  def main (args : Array[String]): Unit ={
    if(args.length == 0){
      println("Please provide the file format you want to read & execute queries \n Available formats: parquet,orc,avro,csv")
    }
    else {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .getOrCreate()

      var df : DataFrame = null
      if(args(0) == "parquet") {
        df = spark.read.parquet("./data/TaxisParquet")
      }
      else if(args(0) == "orc") {
        df = spark.read.orc("./data/TaxisORC")
      }
      else if(args(0) == "avro") {
        df = spark.read.format("com.databricks.spark.avro").load("./data/TaxisAvro")
      }
      else if(args(0) =="csv") {
        df = spark.read.format("csv").option("delimiter",",").option("quote","")
          .option("header", "true").option("inferSchema", "true")
          .load("./data/yellow_tripdata_2017-06.csv")
      }
      else{
        println("Please provide a proper format name")
        return
      }

      df.createOrReplaceTempView("nycTaxis")

      selectStar(spark)

      selectColumns(spark)

      selectColumnsWhere(spark)

      selectCalc(spark)

      selectWithAgg(spark)

    }
  }
}


