import $ivy.`org.apache.spark::spark-core:3.4.1`
import $ivy.`org.apache.spark::spark-sql:3.4.1`
import almond.api.JupyterApi._
import org.apache.spark.rdd._
import org.apache.spark.sql._

import scala.sys.process._
import scala.util.Try

import java.nio.file.Paths

object TrainingExecuteHook extends ExecuteHook {

  private val DataHook  = "%%data"
  private val SqlHook   = "%%sql"
  private val ShellHook = "%%shell"

  override def hook(code: String): Either[ExecuteHookResult, String] = {
    val codeFromFirst          = code.linesIterator.map(_.trim()).dropWhile(_.isEmpty()).toList
    val header: Option[String] = codeFromFirst.headOption
    val codeOnly               = codeFromFirst.drop(1)
    header
      .map { head =>
        if (head.startsWith("%%")) {
          val parts = head.split("\\s+", 2).toList
          val magic = parts(0)
          val entries =
            if (parts.size > 1) {
              parts(1)
                .split("\\s*,\\s*")
                .toList
                .map { e =>
                  val kv = e.split("\\s*=\\s*", 2)
                  kv(0) -> kv.lift(1).getOrElse("")
                }
                .toMap
            } else {
              Map.empty[String, String]
            }
          val limit    = Try("limit=" + entries("limit").toInt).toOption.toList
          val truncate = Try("truncate=" + entries("truncate").toInt).toOption.toList
          val options  = (limit ++ truncate).mkString(", ")

          if (magic == DataHook) {
            val newCode: String =
              codeOnly.reverse
                .dropWhile(_.trim().isEmpty())
                .reverse
                .mkString("\n") + s".showHTML($options)"
            Right(newCode)
          } else if (head == SqlHook) {
            val newCode: String = "spark.sql(\"\"\"" + codeOnly.mkString("\n") + "\"\"\")" + s".showHTML($options)"
            Right(newCode)
          } else if (head == ShellHook) {
            val newCode: String =
              codeOnly
                .map(_.trim())
                .filter(_.nonEmpty)
                .map(command => "spark_helper.shell(\"\"\"" + command + "\"\"\")")
                .mkString("\n")
            Right(newCode)
          } else {
            Left(ExecuteHookResult.Error("unknown hook", s"Hook unknown: $head", List(s"Hook unknown: $head")))
          }
        } else {
          Right(code)
        }
      }
      .getOrElse(Right(code))
  }

}

kernel.addExecuteHook(TrainingExecuteHook)

def shell(command: String, limit: Int = 10): Unit = {
  val cwd    = Paths.get("").toAbsolutePath.toString
  val result = command.split("\\s+").toSeq.lazyLines.take(limit).mkString("\n")

  s"""<pre style="background: black; color: lightgray; padding: 1ex">
<span style="color: cyan">$cwd $$</span> <span style="color: white">$command</span>
$result</pre>
""".asHtml.show()
}

def sparkExport[A](context: AnyRef): Unit = org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(context)

object implicits {

  implicit class RichArray[A](val array: Array[A]) {
    def showHTML(title: String = "", limit: Int = 20, truncate: Int = 120): Unit = {
      import xml.Utility.escape
      val data = array.take(limit).toList
      val rows: Seq[String] =
        data.map { row =>
          val str =
            row match {
              case null                => "null"
              case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
              case array: Array[_]     => array.mkString("[", ", ", "]")
              case it: Iterable[_]     => it.mkString("[", ", ", "]")
              case _                   => row.toString
            }
          if (truncate > 0 && str.length > truncate) {
            // do not show ellipses for strings shorter than 4 characters.
            if (truncate < 4) str.substring(0, truncate)
            else str.substring(0, truncate - 3) + "..."
          } else {
            str
          }
        }

      val titleHTML =
        if (title.isEmpty) ""
        else s"<h3>$title</h3>"

      s"""$titleHTML
          <table class="table">
            ${rows.map { row =>
        s"""<tr><td style="text-align: left">${escape(row)}</td></tr>"""
      }.mkString}
          </table>""".asHtml.show()
    }
  }

  implicit class RichRDD[A](val rdd: RDD[A]) {
    def showHTML(title: String = "", limit: Int = 20, truncate: Int = 120): Unit = {
      rdd.context.setCallSite("showHTML")
      rdd.take(limit).showHTML(title, limit, truncate)
      rdd.context.clearCallSite()
    }
  }

  implicit class RichDS[A](val ds: Dataset[A]) {
    def showHTML(limit: Int = 20, truncate: Int = 120): Unit = {
      import xml.Utility.escape

      val df = ds.toDF()

      df.sparkSession.sparkContext.setCallSite("showHTML")
      val data = df.take(limit).toList
      df.sparkSession.sparkContext.clearCallSite()

      val header = df.schema.fieldNames.toSeq
      val rows: Seq[Seq[String]] =
        data.map { row =>
          row.toSeq.map { cell =>
            val str =
              cell match {
                case null => "null"
                case binary: Array[Byte] =>
                  binary.toList.map("%02X".format(_)).mkString("[", " ", "]")
                case array: Array[_] =>
                  array.mkString("[", ", ", "]")
                case seq: Seq[_] =>
                  seq.mkString("[", ", ", "]")
                case seq: scala.collection.mutable.ArraySeq[_] =>
                  seq.mkString("[", ", ", "]")
                case _ =>
                  cell.toString()
              }
            if (truncate > 0 && str.length > truncate) {
              // do not show ellipses for strings shorter than 4 characters.
              if (truncate < 4) str.substring(0, truncate)
              else str.substring(0, truncate - 3) + "..."
            } else {
              str
            }
          }: Seq[String]
        }

      s"""
<table class="table">
  <tr>
    ${header.map(h => s"<th>${escape(h)}</th>").mkString}
  </tr>
  ${rows.map { row =>
        s"<tr>${row.map(c => s"<td>${escape(c)}</td>").mkString}</tr>"
      }.mkString}
</table>""".asHtml.show()
    }
  }
}
