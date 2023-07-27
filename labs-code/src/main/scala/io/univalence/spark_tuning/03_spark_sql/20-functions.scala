package io.univalence.spark_tuning.`03_spark_sql`

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import io.univalence.spark_tuning.internal.exercise_tools._

import java.sql.{Date, Timestamp}

/**
 * One of the interesting thing with Spark SQL is that it provides a
 * large quantity of functions, in a view to process your data:
 *
 *   - As part of the Spark API:
 *     [[https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html]].
 *   - As builtin SQL functions:
 *     [[https://spark.apache.org/docs/latest/api/sql/index.html]].
 *
 * There are two kinds: aggregate functions and non-aggregate functions.
 * Aggregate function are functions can be used with query including a
 * `groupBy` clause, like `count`, `avg`, `collect_list`... This is not
 * the case for non-aggregated functions (eg. `to_date`, `sqrt`,
 * `regexp_replace`...).
 *
 * The set of functions is sufficient to perform most of your necessary
 * operations. But there are cases where a function might be missing in
 * this set. In this case, Spark SQL allows you to define your own
 * '''UDF''' (for User-Defined Function).
 */
object functions {

  def main(args: Array[String]): Unit =
    time("Functions") {

      val spark =
        SparkSession
          .builder()
          .appName(getClass.getSimpleName)
          .master("local[*]")
          // The timestamp format in data implies to use this config.
          .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
          .config("spark.eventLog.enabled", value = true)
          .getOrCreate()

      import spark.implicits._

      val venuesFilename   = "data/threetriangle/venues.txt.gz"
      val checkinsFilename = "data/threetriangle/checkins.txt.gz"

      val venues =
        spark.read
          .option("sep", "\t")
          .schema("id STRING, latitude DOUBLE, longitude DOUBLE, locationType STRING, country STRING")
          .csv(venuesFilename)
          .as[Venue]

      val checkins =
        spark.read
          .option("sep", "\t")
          // Option to correctly interpret timestamp in checkin data
          .option("timestampFormat", "EEE MMM d HH:mm:ss Z yyyy")
          .schema("userId STRING, venueId STRING, timestamp TIMESTAMP, tzOffset INT")
          .csv(checkinsFilename)
          .as[Checkin]

      val data =
        checkins
          .join(venues.hint("SHUFFLE_HASH"), checkins("venueId") === venues("id"))
          .cache()

      data.createOrReplaceTempView("DATA")

      println(s">>> count: ${data.count()}")

      exercise("Apply a function") {
        exercise_ignore("builtin: timestamp to day") {

          /**
           * To apply a function is as simple as calling a function in
           * Scala.
           *
           * Notice the use of `withColumn` to add a column in the
           * result dataframe.
           */
          val result = data.withColumn("day", to_date($"timestamp"))

          time("builtin: timestamp to day") {
            result.show()
          }

          /**
           * Notice how `to_date` is converted into a `cast` in the
           * physical plan.
           */
          result.explain(extended = true)
        }

        exercise_ignore("Create and apply a UDF: timestamp to day") {

          /**
           * For this example, we will create a function that performs
           * the same thing as `to_date`, for comparison purpose with
           * the previous example.
           */

          def localToDate(timestamp: Timestamp): Date = new Date(timestamp.getTime)

          /**
           * To create a UDF, you have to declare a variable with the
           * function `udf`. `udf` usage starts by indicating the output
           * type and the set of input types. Then you pass the
           * underlying function as a parameter. Do not forget to use
           * `withName` to have an proper output for this UDF, when you
           * will use `show`.
           */
          val local_to_date: UserDefinedFunction = udf[Date, Timestamp](localToDate).withName("local_to_date")

          val result = data.withColumn("day", local_to_date($"timestamp"))

          time("Apply a function") {
            result.show()
          }

          /**
           * This time, `local_to_date` is not converted at all in the
           * physical plan.
           */
          result.explain(extended = true)

          /**
           * This a hint on how Spark SQL is working with builtin
           * functions and UDF.
           *
           * Typically, builtin functions are created by the Spark team.
           * So they know how to optimize them. They can thus determine
           * a cost model for them.
           *
           * This is not the case for UDF, for which no cost model can
           * be applied.
           *
           * On this example, there is no real differences between
           * builtin function approach and UDF, as the quantity of data
           * to process is small. But for larger datasets, you need to
           * consider builtin functions first for performance reason.
           * Create UDF if you do not have the choice.
           */
        }
      }

      exercise("Aggregate") {

        exercise_ignore("Top 5 of most visited location") {
          val result =
            data
              .groupBy(??.asInstanceOf[String])
              // `as` is very helpful to change the name of a column
              .agg(??.asInstanceOf[Column].as("checkins"))
              .orderBy($"checkins".desc)

          time("Top 5 of most visited location") {
            result.show(truncate = false, numRows = 5)
          }
        }

        exercise_ignore("List of all checkins per location") {
          val result =
            data
              .groupBy(??.asInstanceOf[String])
              .agg(??.asInstanceOf[Column].as("checkins"))

          time("List of all checkins per location") {
            result.show(truncate = false)
          }
        }

        exercise_ignore("List of all checkins per location and per day") {
          val result =
            data
              .withColumn("day", ??.asInstanceOf[Column])
              .groupBy(??.asInstanceOf[String])
              .agg(??.asInstanceOf[Column].as("checkins"))
              .orderBy(size($"checkins").desc)

          time("List of all checkins per location and per day") {
            result.show(truncate = false)
          }
        }

        exercise_ignore("List of all first checker of the day for each location") {
          val result =
            data
              .withColumn("day", ??.asInstanceOf[Column])
              .orderBy(??.asInstanceOf[Column])
              .groupBy(??.asInstanceOf[Column])
              .agg(??.asInstanceOf[Column])

          result.printSchema()

          time("List of all first checker of the day for each location") {
            result.show(truncate = false)
          }

          result.explain(extended = true)
        }

      }
    }

}
