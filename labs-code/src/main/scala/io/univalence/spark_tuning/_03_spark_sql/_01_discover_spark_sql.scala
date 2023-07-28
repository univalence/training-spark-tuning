package io.univalence.spark_tuning._03_spark_sql

import org.apache.spark.sql._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.functions._

import io.univalence.spark_tuning.internal.exercise_tools._

/** Now, we will start manipulate CSV data with Spark SQL. */
object _01_discover_spark_sql {

  def main(args: Array[String]): Unit =
    time("Discover Spark SQL") {

      val spark =
        SparkSession
          .builder()
          .appName("discover_spark_sql")
          .master("local[*]")
          /**
           * This config will be used later, to write data in parquet
           * format.
           */
          .config("spark.sql.parquet.compression.codec", "gzip")
          .getOrCreate()

      import spark.implicits._

      val filename = "data/threetriangle/venues.txt.gz"

      val dataframe =
        spark.read
          .option("sep", "\t")
          .schema("id STRING, latitude DOUBLE, longitude DOUBLE, locationType STRING, country STRING")
          .csv(filename)
          /**
           * The line below is used to emulate a cluster of 4 nodes. It
           * will then force to cut the dataset into 4 separated
           * partitions. In this context, a partition means a file.
           */
          .repartition(4)
          .as[Venue]

      dataframe.cache()

      exercise("Simple task") {

        /**
         * OK! Let's manipulate this Spark dataframe. First thing first,
         * we will simply count the number of available rows in the data
         * file.
         *
         * TODO EXERCISE 1: activate the block below and run the program
         * Note: to activate, change `activated` to `true`.
         */
        exercise_ignore("Count") {
          val result1 = time("Count data")(dataframe.count())
          println(s"\t>>> count: $result1")

          check(result1 == 10000L)
        }
      }

      exercise("Simple Query - 1") {

        /**
         * Now, we will display the list of location types.
         *
         * To do so, we will use the Spark API, as below. Every method
         * that appears directly below this comment (`select`,
         * `distinct`), is a method that converts the source dataframe
         * into another dataframe. Those operations are *lazy*: they do
         * not process anything unless you call a method like `count` or
         * `show`, for which you claim a result (those last operations
         * are said to be ''eager'').
         *
         * Notice the use of `$"locationType"` in the `select` method.
         * This is a reference to a column of our dataframe. This
         * notation comes from the statement `import spark.implicits._`
         * in the beginning of this source file. This notation is only
         * available in Scala. There are other notations to reference a
         * column: `col("locationType")` and
         * `dataframe("locationType")`. The last is useful when you have
         * to mix two dataframes, eg. while performing a join.
         *
         * `select` also accepts pure string to reference a column, but
         * in this case, you cannot do complex operations on them, like
         * data transformation, renaming columns, changing their type...
         *
         * TODO EXERCISE 2.1: activate the block below, run the program
         * and note the time.
         */
        exercise_ignore("Query with Spark API") {
          val result2_1 =
            time("Query with Spark API") {
              dataframe
                .select($"locationType")
                .distinct()
            }

          time("Show: Query with Spark API") {
            result2_1.show()
          }

          check(result2_1.count() == 298L)
        }
      }

      /**
       * The line above is really closed to what you would do with SQL.
       * If you remember, we have seen that we can use SQL with Spark
       * SQL (and the name of this Spark module is not innocent).
       *
       * To do so, we first have to declare a named view from our
       * dataframe. This will act as a SQL table.
       */
      dataframe.createOrReplaceTempView("VENUE")

      exercise("Simple Query - 2") {

        /**
         * Then let's request our dataframe from the created view by
         * using the SQL language.
         *
         * TODO EXERCISE 2.2: activate the block below and run the
         * program
         */
        exercise_ignore("Query with SQL") {
          val result2_2 =
            time("Query with SQL") {
              spark.sql(
                """
                  |SELECT DISTINCT locationType
                  |FROM VENUE
                  |""".stripMargin
              )
            }
          time("Show: Query with SQL") {
            result2_2.show()
          }
          check(result2_2.count() == 298L)
        }
      }

      exercise("GROUP BY") {

        /**
         * We now will count the number of venues for each location
         * type.
         *
         * For this exercise we will need to group the rows according to
         * the locationType and then count the rows for each
         * locationType.
         *
         * TODO EXERCISE 3.1: activate the block below, run the program
         * and note the time.
         */
        exercise_ignore("GROUP BY with Spark API") {
          val result3_1 =
            time("GROUP BY with Spark API") {
              dataframe
                .groupBy($"locationType")
                .count()
            }

          time("Show: GROUP BY with Spark API") {
            result3_1.show()
          }

          /**
           * Here, we check the returned dataframe by verifying a part
           * of its content.
           */
          val recordShopCount =
            result3_1
              /**
               * We use a `where` clause to get just on row. Notice the
               * use of triple-equal (`===`) to add constraint and the
               * use `lit` to convert a Scala value into a column
               * expression.
               */
              .where($"locationType" === lit("Record Shop"))
              .head()
              // Then, we extract the count column from the row.
              .getAs[Long]("count")

          // this should display OK in green.
          check(recordShopCount == 24L)
        }

        /**
         * As said before, we can use the Spark SQL DSL or we can
         * directly write SQL query.
         *
         * Below, we will do exactly the same operation as the example
         * above, but with SQL.
         *
         * TODO EXERCISE 3.2: do the same as the previous exercise, but
         * this time use a SQL expression. Has the time spent changed a
         * lot?
         */
        exercise_ignore("GROUP BY with SQL") {
          lazy val result3_2 =
            time("GROUP BY with SQL") {
              // TODO Also replace ??? whith awaiting SQL part
              spark.sql("""
                          |SELECT ???
                          |FROM VENUE
                          |???
                          |""".stripMargin)
            }

          time("Show: GROUP BY with SQL") {
            result3_2.show()
          }

          lazy val recordShopCount =
            result3_2
              .where($"locationType" === lit("Record Shop"))
              .head()
              .getAs[Long]("count")

          check(recordShopCount == 24L)
        }
      }

      exercise("Execution plan") {

        /**
         * As you can do SQL with Spark SQL, through its API or by
         * directly writing SQL queries in string, like any database,
         * you can ask the system to display the execution plan, in
         * order to understand what Spark will exactly do.
         *
         * In fact, whatever the language you use to write your Spark
         * SQL processes, all the statements are converted into symbols
         * and then they follow optimization processes. This something
         * that you will see in the execution plan.
         *
         * TODO EXERCISE 4: activate the block below and run the program
         */
        exercise_ignore("Execution plans in Spark SQL") {
          val result4 =
            dataframe
              .groupBy($"locationType")
              .count()

          // This line will show you 4 execution plans
          result4.explain(ExtendedMode.name)

          /**
           * The execution plan is divided in 4 parts:
           *   - '''Parsed Logical Plan''': this is what Spark as
           *     understood from your query. It sees an aggregation of
           *     data according to `locationType` by counting them. The
           *     data come from a relation (meaning a dataset or a
           *     table) with the CSV format.
           *   - '''Analyzed Logical Plan''': this is the same plan as
           *     the previous one, but Spark has done in addition some
           *     type resolution.
           *   - '''Optimized Logical Plan''': here, Spark cuts the
           *     query into more precise elementary tasks and tries to
           *     optimize them. From the bottom of the plan, Spark will
           *     scan the data file and apply a specific schema. Then
           *     the data are deserialized and put in memory (ie.
           *     directly "understandable" by the JVM), and a projection
           *     on the column locationType is done (meaning that the
           *     other columns are removed). Finally, an aggregation
           *     will be done.
           *   - '''Physical Plan''': this is the final phase of the
           *     optimization plan done by Spark. Here, Sparks adapt the
           *     plan to the physical data support (which depends
           *     especially on the file format).
           *
           * Let's take a closer look at the physical plan. In this
           * plan, you can see in particular that after Spark has loaded
           * data in memory, it will perform two HashAggregates
           * separated by a data Exchange.
           *
           * Especially the data exchange may sound weird, especially if
           * your are on a single machine. But this makes sense when
           * your are processing data distributed on a whole cluster.
           * This data exchange happens because Spark want data of the
           * same group to be in the same partition (and thus in the
           * same node), making further processes on that data easier.
           *
           * All the process ends up by a count. This is what is
           * happening. As an example, let's suppose that the rows for
           * the locationType "Record Shop" are located in two nodes N1
           * and N2. What Spark will do according to our query, it will
           * partially count the quantity of row with `locationType ==
           * "Record Shop"` on N1 and on N2 separately. Then, all the
           * partial count are sent to the same node (say N1), where
           * Spark will finalized the process by adding the partial
           * counts to get the total count.
           */
        }
      }

      /**
       * A last thing to do, is to write down data somewhere, so they
       * can be used by other jobs.
       */
      exercise("Write dataframe in file") {
        val outputname = "data/output/venues"

        /**
         * First, we will save data in CSV file.
         *
         * TODO EXERCISE 5.1: activate the block below and run the
         * program
         */
        exercise_ignore("Write into CSV file") {
          val csvFilename = outputname + ".csv"

          /**
           * As in big data, it is better to respect the principle of
           * immutability, Spark throws an exception if you try to write
           * in the same location again. So, we first clean the
           * workspace.
           */
          clean(csvFilename)

          /**
           * You may notice something weird once the file is written: it
           * is not a file, but a directory, that contains many files.
           *
           * First, for every written file in this directory, there is a
           * CRC file, that contains a checksum. Then there is an empty
           * `_SUCCESS` file created once the write job is done.
           *
           * And then, you have series of files with a name that looks
           * like `part-<partition_number>-<uuid>-c000.csv`. This tends
           * to reveal how Sparks work.
           *
           * When you process data in a Spark application, Spark will
           * divide your application into ''jobs''. A job is delimited
           * by a method of type action, like `show`, `count`, or
           * `write`. They are eager operations, ie. for which a result
           * is immediately needed. The jobs are then cut into
           * elementary '''tasks'''. All the tasks and their
           * dependencies generate a '''DAG''' (for Direct Acyclic
           * Graph). This graph helps Spark to manage, distribute, and
           * schedule tasks among the nodes of the Spark cluster. If
           * your data are divided into partitions, the DAG is rerun as
           * is for each partition in separated processes or separated
           * threads.
           *
           * This include the write operation. As we have 4 partitions,
           * Spark will convert the write command into 4 write
           * operations (one for each partition). That is why, your data
           * is written in 4 different files.
           *
           * If you want just one file, you have to change the number of
           * partitions to 1, with operations like `repartition` or
           * `coalesce`.
           */
          time("Write into CSV file") {
            dataframe.write.csv(csvFilename)
          }

          /**
           * Notice that Spark has no problem to read data from such
           * file organization. By doing
           * `spark.read.csv("data/output/venues.csv")`, you will
           * retrieve your data.
           */
        }

        /**
         * Next step: write data into Parquet.
         *
         * TODO EXERCISE 5.2: activate the block below and run the
         * program
         */
        exercise_ignore("Write into Parquet file") {
          val parquetFilename = outputname + ".parquet"

          clean(parquetFilename)

          /**
           * Parquet is a columnar binary file format: the data are
           * organized by column first, instead of being organized by
           * row, like many file format. This is almost efficient when
           * you want to query some specific columns only.
           *
           * Parquet proposes an efficient approach for compressed data.
           * As we have seen at the beginning of this file, we have
           * configured Spark to apply the GZip codec to Parquet files.
           *
           * {{{
           *   .config("spark.sql.parquet.compression.codec", "gzip")
           * }}}
           *
           * Their other compression codec available (uncompressed,
           * snappy, gzip, lzo, brotli, lz4, zstd).
           */
          time("Write into Parquet file") {
            dataframe.write.parquet(parquetFilename)
          }
        }
      }
    }

}
