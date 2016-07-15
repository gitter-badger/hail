package org.broadinstitute.hail.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._

import scala.io.Source

object TextTableConfiguration {
  final val defaultTypes = Map.empty[String, Type]
  final val defaultSelection: Option[Seq[String]] = None
  final val defaultSep = "\t"
  final val defaultMissing = "NA"
  final val defaultComment: Option[String] = None
  final val defaultNoHeader: Boolean = false
}

case class TextTableConfiguration(
  types: Map[String, Type] = TextTableConfiguration.defaultTypes,
  selection: Option[Seq[String]] = TextTableConfiguration.defaultSelection,
  commentChar: Option[String] = TextTableConfiguration.defaultComment,
  separator: String = TextTableConfiguration.defaultSep,
  missing: String = TextTableConfiguration.defaultMissing,
  noHeader: Boolean = TextTableConfiguration.defaultNoHeader)

object TextTableReader {

  val booleanRegex = """([Tt]rue)|([Ff]alse)|(TRUE)|(FALSE)""".r
  val integerRegex = """\d+""".r
  val doubleRegex = """(-?\d+(\.\d+)?)|(-?(\d*)?\.\d+)|(-?\d+(\.\d+)?[eE]-\d+""".r

  def read(sc: SparkContext,
    files: Array[String],
    config: TextTableConfiguration): (TStruct, RDD[WithContext[Annotation]]) = read(sc, files, config.types,
    config.selection, config.commentChar, config.separator, config.missing, config.noHeader)

  def read(sc: SparkContext,
    files: Array[String],
    types: Map[String, Type] = TextTableConfiguration.defaultTypes,
    selection: Option[Seq[String]] = TextTableConfiguration.defaultSelection,
    commentChar: Option[String] = TextTableConfiguration.defaultComment,
    separator: String = TextTableConfiguration.defaultSep,
    missing: String = TextTableConfiguration.defaultMissing,
    noHeader: Boolean = TextTableConfiguration.defaultNoHeader): (TStruct, RDD[WithContext[Annotation]]) = {
    require(files.nonEmpty)

    val firstFile = files.head
    val firstLine = readFile(firstFile, sc.hadoopConfiguration) { dis =>
      val lines = Source.fromInputStream(dis)
        .getLines()
        .filter(line => commentChar.forall(pattern => !line.startsWith(pattern)))

      if (lines.isEmpty)
        fatal(
          s"""unreadable file: empty
              |  Offending file: `$firstFile'
           """.stripMargin)
      lines.next()
      //      (lines.next(), lines.take(20))
    }

    val columns = if (noHeader) {
      firstLine.split(separator)
        .zipWithIndex
        .map { case (_, i) => s"_$i" }
    }
    else
      firstLine.split(separator).map(unescapeString)

    val duplicates = columns.duplicates()
    if (duplicates.nonEmpty) {
      fatal(s"invalid header: found duplicate columns [${duplicates.map(x => '"' + x + '"').mkString(", ")}]")
    }

    types.foreach { case (k, v) =>
      if (!TableAnnotationImpex.supportsType(v))
        fatal(
          s"""invalid type `$v' for column `$k'
              |  Supported types: ${
            TableAnnotationImpex.supportedTypes.map(_.toString)
              .toArray
              .sorted
              .mkString(", ")
          }""".stripMargin)
    }

    val filter: (String) => Boolean = (line: String) => {
      if (noHeader)
        true
      else line != firstLine
    } && commentChar.forall(ch => !line.startsWith(ch))

    val nField = columns.length
    def checkLength(arr: Array[String]) {
      if (arr.length != nField)
        fatal(s"expected $nField fields, but found ${arr.length} fields")
    }

    val namesAndTypes = columns.map(name => (name, types.getOrElse(name, TString)))
    val schema = TStruct(namesAndTypes: _*)

    val rdd = sc.textFilesLines(files)
      .filter(line => filter(line.value))
      .map {
        _.map { line =>
          val split = line.split(separator)
          checkLength(split)
          Annotation.fromSeq(
            for (elem <- split; (name, t) <- namesAndTypes) yield {
              try {
                if (elem == missing)
                  null
                else
                  TableAnnotationImpex.importAnnotation(elem, t)
              }
              catch {
                case e: Exception =>
                  fatal(s"""${e.getClass.getName}: could not convert "$elem" to $t in column "$name" """)
              }
            })
        }
      }


    (schema, rdd)
  }
}
