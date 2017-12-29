package pl.edu.agh.inz.ml

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object NYCTaxisLinRegCSVinRDD {

  def parseTaxis(line : String)= {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val fields = line.split(",")
    LabeledPoint(fields(13).toDouble, Vectors.dense(Array(fields(3).toInt, fields(4).toDouble, fields(7).toInt,fields(8).toInt, fields(9).toInt,
      fields(10).toDouble, fields(11).toDouble,fields(12).toDouble, fields(13).toDouble,fields(14).toDouble, fields(15).toDouble,fields(16).toDouble)))
  }


  def main(args : Array[String]): Unit ={

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "NYCTaxis")

    val rootDirectory = "./data"

    val data = sc.textFile(rootDirectory + "/yellow_tripdata_2017-06.csv")

    //find header
    val header = data.first()

    //filter out the header
    val taxiFiltered= data.filter(row => row != header)

    //make an RDD
    val parsedData = taxiFiltered.filter(line => !(line.isEmpty)).map(parseTaxis).cache()


    // Building the model
    val numIterations = 100
    val stepSize = 0.0001

    val splits = parsedData randomSplit Array(0.8, 0.2)

    val training = splits(0) cache
    val test = splits(1) cache

    val model = LinearRegressionWithSGD.train(training, numIterations, stepSize)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)

    valuesAndPreds.take(100).foreach(println)
  }

}
