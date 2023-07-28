package io.univalence.spark_tuning._05_mllib

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

object _10_kmean {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("MlLib - K-means")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val training_data =
      Seq(
        Array(-1.0, 2.0),
        Array(-1.0, 3.0),
        Array(-2.0, 2.0),
        Array(1.0, 2.0),
        Array(1.0, 3.0),
        Array(2.0, 2.0),
        Array(1.0, -2.0),
        Array(1.0, -3.0),
        Array(2.0, -2.0)
      ).toDF("features")

    // create k-means algorithm and set parameters
    val kmeans: KMeans = new KMeans().setK(3).setSeed(1)

    // train the model
    val model: KMeansModel = kmeans.fit(training_data)

    // show the location of the different location of the centroids
    println("Cluster Centers:")
    model.clusterCenters.foreach(println)
    // show for each point of the training data the centroid number it
    // belongs to
    println("Predictions on training data:")
    model.summary.predictions.show()

    // create an evaluator to check the consistency of the model
    val evaluator = new ClusteringEvaluator()
    // get the consistency
    // the value should be above 0.5
    val silhouette: Double = evaluator.evaluate(model.summary.predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // get new data to do prediction on
    val data =
      Seq(
        Array(1.0, -2.0),
        Array(1.0, -3.0),
        Array(-1.0, 2.0),
        Array(2.0, 5.0)
      ).toDF("features")

    // do prediction and show the result
    val prediction: DataFrame = model.transform(data)
    prediction.show()
  }
}
