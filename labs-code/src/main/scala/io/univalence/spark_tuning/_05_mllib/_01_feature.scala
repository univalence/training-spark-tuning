package io.univalence.spark_tuning._05_mllib

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

object _01_feature {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("MlLib - Features")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val sentenceData =
      Seq(
        (0.0, "Hello, how are you?"),
        (1.0, "Bonjour, comment ça va?"),
        (2.0, "Hola, ¿cómo estás?")
      ).toDF("label", "sentence")

    // cut sentences into words
    val tokenizer: Tokenizer =
      new Tokenizer()
        .setInputCol("sentence")
        .setOutputCol("words")
    val wordsData: DataFrame = tokenizer.transform(sentenceData)
    wordsData.show(truncate = false)

    val hashingTF =
      new HashingTF()
        .setInputCol("words")
        .setOutputCol("rawFeatures")
        .setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show(truncate = false)
    featurizedData.printSchema()

    val idf =
      new IDF()
        .setInputCol("rawFeatures")
        .setOutputCol("features")
        .setMinDocFreq(2)
    val idfModel: IDFModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "sentence", "features").show(truncate = false)
  }
}
