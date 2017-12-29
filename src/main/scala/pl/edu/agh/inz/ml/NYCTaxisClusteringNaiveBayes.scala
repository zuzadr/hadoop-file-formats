package pl.edu.agh.inz.ml

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object NYCTaxisClusteringNaiveBayes {

  case class NYCData(VendorID : Int,tpep_pickup_datetime : Date,tpep_dropoff_datetime : Date,passenger_count : Int,trip_distance : Double,RatecodeID : Int,store_and_fwd_flag : String,PULocationID : Int,DOLocationID: Int,payment_type : Int,fare_amount: Double,
                     extra : Double,mta_tax : Double,tip_amount: Double,tolls_amount :Double ,improvement_surcharge: Double,total_amount : Double)

  def parseTaxis(line : Row): NYCData = {
    val fields = line.mkString(",").split(",")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if(fields(12).toDouble > 0.0)
      return new NYCData(fields(0).toInt, new Date(format.parse(fields(1)).getTime),new Date(format.parse(fields(2)).getTime), fields(3).toInt,fields(4).toDouble, fields(5).toInt,fields(6), fields(7).toInt,fields(8).toInt, fields(9).toInt,
        fields(10).toDouble, fields(11).toDouble,fields(12).toDouble, fields(13).toDouble,fields(14).toDouble, fields(15).toDouble,fields(16).toDouble)
    else
      return new NYCData(fields(0).toInt, new Date(format.parse(fields(1)).getTime),new Date(format.parse(fields(2)).getTime), fields(3).toInt,fields(4).toDouble, fields(5).toInt,fields(6), fields(7).toInt,fields(8).toInt, fields(9).toInt,
        fields(10).toDouble, fields(11).toDouble,0.0, fields(13).toDouble,fields(14).toDouble, fields(15).toDouble,fields(16).toDouble)
  }

  def main(args: Array[String]): Unit = {
    if(args.length == 0 ) {
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


      val rootDirectory = "./data"

      var taxiFile : DataFrame = null
      if(args(0) == "csv"){
        taxiFile = spark.read.format("csv").option("delimiter",",").option("quote","").option("header", "true").option("inferSchema", "true").load(rootDirectory+"/yellow_tripdata_2017-06.csv")
      }
      else if(args(0) == "parquet"){
        taxiFile = spark.read.parquet(rootDirectory + "/TaxisParquet")
      }
      else if(args(0) == "orc"){
        taxiFile = spark.read.orc(rootDirectory + "/TaxisORC")
      }
      else if(args(0) == "avro"){
        taxiFile = spark.read.format("com.databricks.spark.avro").load(rootDirectory + "/TaxisAvro")
      }
      else {
        return
      }

      val dataToTrain = taxiFile.select("tip_amount").rdd

      val taxisMl = dataToTrain.map(r => Vectors.dense(r.mkString.toDouble)).cache()

      //train the model based on the data - 5 clusters, 10 iterations
      val startTime = System.nanoTime()
      val model = KMeans.train(taxisMl,5,10)
      val endTime = System.nanoTime()

      println("Learning K-Means in " + args(0) + " format took " + (endTime- startTime) + " nanoseconds")

      val clusterCenters = model.clusterCenters.map(_.toArray).map(lines => lines(0)).sorted

      println("cluster centers: ")
      clusterCenters.foreach(println)
      println("------------")

      val clustersRDD = model.predict(taxisMl)

      import spark.implicits._

      val taxiss = taxiFile.rdd.map(parseTaxis)
      val taxiClusters = taxiss.zip(clustersRDD).map(value => (value._1.VendorID,value._1.tpep_pickup_datetime,
        value._1.tpep_dropoff_datetime,value._1.passenger_count,value._1.trip_distance,value._1.RatecodeID, value._1.store_and_fwd_flag,
        value._1.PULocationID,value._1.DOLocationID,value._1.passenger_count,value._1.fare_amount,value._1.extra,value._1.mta_tax,
        value._1.tip_amount,value._1.tolls_amount, value._1.improvement_surcharge,value._1.total_amount,value._2))

      val taxisNew = taxiClusters.toDF("VendorID"
        ,"tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID","payment_type"
        ,"fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","label")

      /**
      features taken for Bayes algorithm:
        - trip_distance
        - fare_amount
        - total_amount
        - tip_amount
        */

      /**
        * Some statistics after clustering:
            println("min and max for each cluster")
            toBayes.groupBy("label").max("tip_amount").show()
            toBayes.groupBy("label").min("tip_amount").show()

            println("counts of each cluster")
            toBayes.groupBy("label").count().show()

        */

      //filter out bad records
      val toBayes = taxisNew.filter($"trip_distance"> 0.0).filter($"fare_amount" > 0.0).filter($"total_amount" > 0.0).filter($"tip_amount" > 0.0)

      // exclude other columns
      val ignored = List("VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID","payment_type"
        ,"extra","mta_tax","tolls_amount","improvement_surcharge","label")

      val featInd = toBayes.columns.diff(ignored).map(toBayes.columns.indexOf(_))

      // Get index of target
      val targetInd = toBayes.columns.indexOf("label")

      featInd.filter(value => value > 0.0 )

      val data4Bayes = toBayes.rdd.map(r => LabeledPoint(r.getInt(targetInd),Vectors.dense(featInd.map(r.getDouble(_)))))

      // Split data into training (60%) and test (40%).
      val Array(training, test) = data4Bayes.randomSplit(Array(0.6, 0.4))


      val s= System.nanoTime()
      val bayesModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
      val  e = System.nanoTime()

      println("Training with Naive Bayes in " + args(0) + " format took " + (e-s) + " nanoseconds")

      val predictionAndLabel = test.map(p => (bayesModel.predict(p.features), p.label))
      val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

      println("bayes done. Accuracy: " + accuracy)

    }
  }

}
