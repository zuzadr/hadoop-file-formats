package pl.edu.agh.inz.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object NYCTaxisLinReg {

  def isAllDigits(x: String) = x forall Character.isDigit

  def parseTaxis(line : Row)= {
    val fields = line.mkString(",").split(",")
    //filter out bad records
    fields.filter(isAllDigits).filter(ch => ch.toDouble > 0.0)
    LabeledPoint(fields(3).toInt, Vectors.dense(Array(fields(3).toInt, fields(4).toDouble, fields(7).toInt,fields(8).toInt, fields(9).toInt,
      fields(10).toDouble, fields(11).toDouble,fields(12).toDouble, fields(13).toDouble,fields(14).toDouble, fields(15).toDouble,fields(16).toDouble)))
  }


  def main(args: Array[String]): Unit = {
    if(args.length == 0 ){
      println("Please provide format, in which you want to preform clustering & Naive Bayes \n Available formats : csv,parquet,orc,avro")
    }
    else{
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      // Use new SparkSession interface in Spark 2.0
      val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .getOrCreate()

      val rootDirectory = "/Users/zdrwila/Documents/Inz/data"
      var df : DataFrame = null
      if(args(0) =="csv") df = spark.read.format("csv").option("delimiter",",")
        .option("quote","").option("header", "true").option("inferSchema", "true")
        .load(rootDirectory+"/yellow_tripdata_2017-06.csv")
      else if(args(0) == "parquet") df = spark.read.parquet(rootDirectory + "/TaxisParquet")
      else if(args(0) == "orc") df = spark.read.orc(rootDirectory + "/TaxisORC")
      else if(args(0) == "avro") df = spark.read.format("com.databricks.spark.avro").load(rootDirectory + "/TaxisAvroDeflate")
      else {
        println("Please provide a valid file format")
        return
      }
      val dataToTrain = df.rdd.map(parseTaxis)

      // Building the model
      val numIterations = 100
      val stepSize = 0.0001

      val splits = dataToTrain randomSplit Array(0.8, 0.2)

      val training = splits(0) cache
      val test = splits(1) cache

      val startTime = System.nanoTime()
      val model = LinearRegressionWithSGD.train(training, numIterations, stepSize)
      val stopTime = System.nanoTime()

      println("Training a model in " + args(0) + " format took " + (stopTime - startTime) + " ns")

      // Evaluate model on training examples and compute training error
      val valuesAndPreds = test.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
      println("training Mean Squared Error = " + MSE)

      //cast predicted values, so that they represent a predicted integer number
      valuesAndPreds.map(l => (l._1, Math.ceil(l._2))).take(100).foreach(println)
    }
  }

}
