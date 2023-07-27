ThisBuild / scalaVersion := "2.13.8"

val libVersion =
  new {
    val spark = "3.4.1"
    val kafka = "3.5.0"
  }

lazy val root =
  (project in file("."))
    .settings(
      name := "training-spark-tuning"
    )
    .aggregate(
      `labs-code`,
      `labs-macro`
    )

lazy val `labs-code` =
  (project in file("labs-code"))
    .settings(
      name := "labs-code",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core"                 % libVersion.spark,
        "org.apache.spark" %% "spark-sql"                  % libVersion.spark,
        "org.apache.spark" %% "spark-mllib"                % libVersion.spark,
        "org.apache.spark" %% "spark-streaming"            % libVersion.spark,
        "org.apache.spark" %% "spark-streaming-kafka-0-10" % libVersion.spark,
        "org.apache.spark" %% "spark-sql-kafka-0-10"       % libVersion.spark,
        "org.apache.kafka"  % "kafka-clients"              % libVersion.kafka
      ),
      javaOptions ++= Seq(
        "--add-exports",
        "java.base/sun.nio.ch=ALL-UNNAMED"
      )
    )
    .dependsOn(`labs-macro`)

lazy val `labs-macro` =
  (project in file("labs-macro"))
    .settings(
      name := "labs-macro",
      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % scalaVersion.value
      )
    )
