package pl.edu.agh.inz.queries

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GithubQueries {

  def main (args : Array[String]): Unit ={
    if(args.length == 0){
      println("Please provide the file format you want to read & execute queries \n Available formats: parquet,orc,avro,json")
    }
    else {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .getOrCreate()

      if(args(0) == "parquet") {
        val df = spark.read.parquet("/Users/zdrwila/Documents/Inz/data/GithubParquet")
        df.createOrReplaceTempView("github")
      }
      else if(args(0) == "orc") {
        val df = spark.read.orc("/Users/zdrwila/Documents/Inz/data/GithubORC")
        df.createOrReplaceTempView("github")
      }
      else if(args(0) == "avro") {
        val df = spark.read.format("com.databricks.spark.avro").load("/Users/zdrwila/Documents/Inz/data/GithubAvro")
        df.createOrReplaceTempView("github")
      }
      else if(args(0) =="json") {
        val df = spark.read.parquet("/Users/zdrwila/Documents/Inz/data/2017-06-01-8-12.json")
        df.createOrReplaceTempView("github")
      }
      else{
        return
      }

      //projekcja całego zbioru
      val t1 = System.nanoTime()
      val sqlDf = spark.sql("select * from github")
      //    sqlDf.show()
      val t0 = System.nanoTime()
      println("Projekcja całego zbioru " + (t0 - t1) + " ns")

      //projekcja poszczególnych kolumn
      val t3 = System.nanoTime()
      spark.sql("select id,repo from github")
      val t2 = System.nanoTime()
      println("Projekcja poszczególnych kolumn " + (t2 - t3) + " ns")

      //projekcja poszczególnych kolumn z warunkiem
      val t5 = System.nanoTime()
      spark.sql("select id,repo from github where type = \"PushEvent\"")
      val t4 = System.nanoTime()
      println("Projekcja poszczególnych kolumn z warunkiem " + (t4 - t5) + " ns")

      //projekcja zagnieżdzona
      val t7 = System.nanoTime()
      spark.sql("select payload.action,payload.issue.assignee.login,actor.login,created_at,repo.name,repo.url from github")
      val t6 = System.nanoTime()
      println("Projekcja zagnieżdzona " + (t6 - t7) + " ns")

      //projekcja z użyciem funkcji agregujących
      val t9 = System.nanoTime()
      spark.sql("select count(*),type from github group by type")
      val t8 = System.nanoTime()
      //    spark.sql("select count(*),type from github group by type").show()
      println("Funkcje agregujące  " + (t8 - t9) + " ns")
    }
  }
}

