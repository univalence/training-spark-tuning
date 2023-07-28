package io.univalence.spark_tuning._03_spark_sql

import org.apache.spark.sql._

import io.univalence.spark_tuning.internal.exercise_tools._

/**
 * In this file, you will find supervised exercises to discover '''Spark
 * SQL'''.
 *
 * Spark SQL aims to provide a tool to process structured data typically
 * with a large volume, in a Spark cluster or locally. It is inspired by
 * the Python library Pandas, except that Spark SQL provides the
 * distribution aspect.
 *
 * Spark SQL is one module in the Apache Spark galaxy, along with Spark
 * Core, Spark MLlib (for machine learning), Spark Streaming (for
 * (look-alike) stream processing), or Spark GraphX (for distributed
 * graph processing, like PageRank algorithm, originally developed for
 * Google search engine).
 *
 * Spark SQL is the most used module in Apache Spark by data engineers.
 * It can be used along with many languages: Scala (native
 * implementation), Java (with a poorer implementation), Python (by
 * using the network gateway Py4J), R (another language used by data
 * scientists, communicating with Spark by using an in-house network
 * gateway), and SQL (with an integrated interpreter).
 *
 * @see
 *   https://spark.apache.org/docs/3.3.1/sql-programming-guide.html
 */
object _00_discover_spark_sql__read_a_csv_file {

  def main(args: Array[String]): Unit =
    time("Discover Spark SQL - Read a CSV file") {

      /**
       * To use Spark, you have to first create a Spark session. A Spark
       * session, is a kind of context, where Spark will start the
       * communication with the Spark cluster or other kind of cluster
       * (if one is configured). It is also used to configure Spark, and
       * to initiate the Spark application.
       */
      val spark =
        SparkSession
          .builder()
          /**
           * When you create a Spark session, you have to give a name to
           * your Spark application. This name will be used in logs and
           * in the monitoring interface. It is important enough to
           * retrieve the metrics of your Spark application in certain
           * Spark tools.
           */
          .appName("discover_spark_sql__read_a_csv_file")
          /**
           * You also have to indicate the master node of your Spark
           * cluster. It is a string that can have many forms:
           *
           *   - `spark://<hostname>:<port>`, for standalone mode.
           *   - `yarn` (yes, just `yarn`), if the Spark cluster is
           *     deployed on Hadoop. In this case, you have to configure
           *     `HADOOP_CONF_DIR` or `YARN_CONF_DIR` to locate the file
           *     with the full configuration to find and describe the
           *     Hadoop cluster.
           *   - `k8s://<hostname>:<port>`, if the Spark cluster is
           *     deployed on Kubernetes.
           *
           * If you only want to run Spark locally, with no Spark
           * cluster, you can specify `local[*]` for the master, like
           * below. The `*` indicates that Spark will use the available
           * CPU cores for its processing. Instead of `*`, you can
           * specify a number, eg. `local[2]` to limit Spark to 2 CPU
           * cores.
           */
          .master("local[*]")
          .getOrCreate()

      /**
       * Once the Spark session is created, we import all the implicit
       * declarations from this session, that will make our daily life
       * easier as a data engineer.
       */

      /**
       * Here comes the interesting part!
       *
       * The file below comes with compressed TSV data
       * (Tabulation-Separated Data, like CSV, but with tabulation as a
       * separator). Spark SQL is able to handle different kinds of file
       * format compressed or not, binary or not (eg. CSV, JSON, Avro,
       * Parquet, ORC...), and much more. As we are in the big data
       * domain, the file to read might be partition into pieces
       * scattered all other a cluster. Input data might not even been a
       * file. Data can come from a database or message queue. If your
       * data format is not supported, plugins are available, and you
       * can even provide your own implementation.
       *
       * The data in the file below are about different popular
       * locations, with their id, coordinates, their type, and their
       * country. It contains 10'000 lines.
       */
      val filename = "data/threetriangle/venues.txt.gz"

      /**
       * We will use Spark SQL to read the content of the file. As you
       * can notice, we ask Spark to interpret the content of the file
       * as a CSV format (for Comma-Separated Values).
       *
       * As said above, Spark SQL is able to read many file formats,
       * like human-readable formats (CSV, JSON file), binary formats
       * like Avro, binary formats dedicated to partitioned/distributed
       * data like Parquet or ORC. It can also read any other kind of
       * data sources, like JDBC sources, Hive, Kafka (with Spark
       * Streaming)...
       *
       * The expression below returns a dataframe.
       *
       * A '''dataframe''' is a structure that acts like a collection,
       * but it does not necessary mount data in memory. A dataframe is
       * essentially ''lazy''. This is very welcome, especially if you
       * have terabytes of data to process. Here, lazy means that you
       * will only do necessary processes according to the context.
       *
       * A dataframe divides data into rows and columns (like in a table
       * in a database). A dataframe comes with a schema: all columns
       * have a name and a type. All data in the dataframe should
       * conform to this schema.
       */
      val dataframe =
        spark.read
          .csv(filename)

      /**
       * This line below asks Spark to keep the content of the file in
       * memory, so Spark does not have to reload the content for every
       * single actions we are doing on our dataframe.
       */
      dataframe.cache()

      exercise("Show data") {

        /**
         * Now, we ask Spark to display a sample of what it has read.
         */
        time("show dataframe") {
          dataframe.show()
        }

        /**
         * We can also ask Spark to display the schema it has determined
         * from what it has read.
         */
        dataframe.printSchema()
      }

      // NOW, RUN THIS PROGRAM!!! (it might take some times, but it is OK!)

      // ...
      // ...
      // ...
      // ...

      /**
       * At this stage, you will find the results of `show()` and
       * `printSchema()`.
       *
       * Note the time it takes just to show data and the time it takes
       * run the whole program.
       *
       * ...
       *
       * Well, the result might not be really convincing in terms of
       * diplayed data... :/
       */

      /**
       * Let's making things better!
       *
       * The first thing we will do is to ask Spark not to use the comma
       * (`,`) as a separator, but to use the tabulation (`\t`) instead.
       *
       * TODO EXERCISE 1: in the file read statement, insert the line
       * `.option("sep", "\t")` and run the program. Also, take note of
       * the time spent.
       */

      // ...
      // ...
      // ...
      // ...

      /**
       * It should be better. But still! The column name is
       * unintelligible and the type of the coordinate columns does not
       * correspond. We need here to provide a schema.
       *
       * There are two possibilities:
       *
       *   - use the Scala API
       *   - describe the schema in a string
       *
       * We will the second option.
       *
       * TODO EXERCISE 2: in the file read statement, insert the line
       * .schema("id STRING, latitude DOUBLE, longitude DOUBLE,
       * locationType STRING, country STRING") and run the program.
       */

      // ...
      // ...
      // ...
      // ...

      /**
       * This should be far better!
       *
       * We will do one last optional thing, that you can only do in
       * Scala and not in Python. But doing this, it can sometimes make
       * your developer's life easier.
       *
       * If you look in the current directory, you will see a case class
       * that matches with the content of the file we are reading. This
       * case class is [[Venue]]. So, what we will do is to turn the
       * resulting dataframe into a `Dataset[Venue]`.
       *
       * TODO EXERCISE 3: at the end of the file read statement, add the
       * line `.as[Venue]` and run the program.
       */

      // ...
      // ...
      // ...
      // ...

      /**
       * It does not change a thing in the output. But things will be
       * easier when we will have to manipulate data with the Spark
       * representation.
       *
       * You also may have notice that it takes more time to collect
       * your data from the file.
       *
       * TODO How would you explain such difference in time?
       */

    }

}
