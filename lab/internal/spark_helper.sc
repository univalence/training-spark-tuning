import $ivy.`org.apache.spark::spark-core:3.3.2`
import $ivy.`org.apache.spark::spark-sql:3.3.2`

import org.apache.spark.rdd._
import org.apache.spark.sql._
import scala.sys.process._
import java.nio.file.Paths

def shell(command: String, limit: Int=10): Unit = {
    val cwd = Paths.get("").toAbsolutePath.toString
    val result = command.split("\\s+").toSeq.lazyLines.take(limit).mkString("\n")

    publish.html(s"""
    <pre style="background: black; color: lightgray; padding: 1ex">
<span style="color: cyan">$cwd $$</span> <span style="color: white">$command</span>
$result</pre>""")
}

implicit class RichArray[A](val array: Array[A]) {
    def showHTML(title: String="", limit:Int = 20, truncate: Int = 20) = {
    import xml.Utility.escape
    val data = array.take(limit)
    val rows: Seq[String] = data.map { row =>
        val str = row match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => row.toString
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

    publish.html(s"""$titleHTML
      <table class="table">
        ${rows.map { row =>
          s"""<tr><td style="text-align: left">${escape(row)}</td></tr>"""
        }.mkString
        }
      </table>""")
    }
}

implicit class RichRDD[A](val rdd: RDD[A]) {
  def showHTML(title: String="", limit:Int = 20, truncate: Int = 20) = {
    rdd.context.setCallSite("showHTML")
    rdd.take(limit).showHTML(title, limit, truncate)
    rdd.context.clearCallSite()
  }
}


implicit class RichDS[A](val ds: Dataset[A]) {
  def showHTML(limit:Int = 20, truncate: Int = 20) = {
    import xml.Utility.escape
    val df = ds.toDF

    df.sparkSession.sparkContext.setCallSite("showHTML")
    val data = df.take(limit)
    df.sparkSession.sparkContext.clearCallSite()

    val header = df.schema.fieldNames.toSeq
    val rows: Seq[Seq[String]] = data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] =>
            binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] =>
            array.mkString("[", ", ", "]")
          case seq: Seq[_] =>
            seq.mkString("[", ", ", "]")
          case seq: scala.collection.mutable.ArraySeq[_] =>
            seq.mkString("[", ", ", "]")
          case _ =>
            cell.toString
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

    publish.html(s"""
      <table class="table">
        <tr>
        ${header.map(h => s"<th>${escape(h)}</th>").mkString}
        </tr>
        ${rows.map { row =>
          s"<tr>${row.map { c => s"<td>${escape(c)}</td>" }.mkString}</tr>"
        }.mkString
        }
      </table>""")
  }
}