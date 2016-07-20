package org.broadinstitute.hail.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._

object TextTableConfiguration {
  final val defaultTypes = Map.empty[String, Type]
  final val defaultSep = "\t"
  final val defaultMissing = "NA"
  final val defaultComment: Option[String] = None
  final val defaultNoHeader: Boolean = false
  final val defaultNoImpute: Boolean = false
}

case class TextTableConfiguration(
  types: Map[String, Type] = TextTableConfiguration.defaultTypes,
  commentChar: Option[String] = TextTableConfiguration.defaultComment,
  separator: String = TextTableConfiguration.defaultSep,
  missing: String = TextTableConfiguration.defaultMissing,
  noHeader: Boolean = TextTableConfiguration.defaultNoHeader,
  noImpute: Boolean = TextTableConfiguration.defaultNoImpute)

object TextTableReader {

  val booleanRegex = """^([Tt]rue)|([Ff]alse)|(TRUE)|(FALSE)$"""
  val intRegex = """^-?\d+$"""
  val doubleRegex = """^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$"""
  val variantRegex = """^.+:\d+:[ATGC]+:([ATGC]+|\*)(,([ATGC]+|\*))*$"""
  val headToTake = 20

  def guessType(values: Seq[String], missing: String): Option[Type] = {
    require(values.nonEmpty)

    val size = values.size
    val booleanMatch = values.exists(value => value.matches(booleanRegex))
    val allBoolean = values.forall(value => value.matches(booleanRegex) || (value == missing))

    val variantMatch = values.exists(value => value.matches(variantRegex))
    val allVariant = values.forall(value => value.matches(variantRegex) || (value == missing))

    val doubleMatch = values.exists(value => value.matches(doubleRegex))
    val allDouble = values.forall(value => value.matches(doubleRegex) || (value == missing))

    val intMatch = values.exists(value => value.matches(intRegex))
    val allInt = values.forall(value => value.matches(intRegex) || (value == missing))

    if (values.forall(_ == missing))
      None
    else if (allBoolean && booleanMatch)
      Some(TBoolean)
    else if (allVariant && variantMatch)
      Some(TVariant)
    else if (allInt && intMatch)
      Some(TInt)
    else if (allDouble && doubleMatch)
      Some(TDouble)
    else
      Some(TString)
  }

  def read(sc: SparkContext,
    files: Array[String],
    config: TextTableConfiguration): (TStruct, RDD[WithContext[Annotation]]) = read(sc, files, config.types,
    config.commentChar, config.separator, config.missing, config.noHeader, config.noImpute)

  def read(sc: SparkContext,
    files: Array[String],
    types: Map[String, Type] = TextTableConfiguration.defaultTypes,
    commentChar: Option[String] = TextTableConfiguration.defaultComment,
    separator: String = TextTableConfiguration.defaultSep,
    missing: String = TextTableConfiguration.defaultMissing,
    noHeader: Boolean = TextTableConfiguration.defaultNoHeader,
    noImpute: Boolean = TextTableConfiguration.defaultNoImpute): (TStruct, RDD[WithContext[Annotation]]) = {
    require(files.nonEmpty)

    val firstFile = files.head
    val firstLines = readLines(firstFile, sc.hadoopConfiguration) { lines =>
      val filt = lines
        .filter(line => commentChar.forall(pattern => !line.value.startsWith(pattern)))

      if (filt.isEmpty)
        fatal(
          s"""invalid file: no lines remaining after comment filter
              |  Offending file: `$firstFile'
           """.stripMargin)
      else
        filt.take(headToTake).toArray
    }

    val firstLine = firstLines.head.value

    val columns = if (noHeader) {
      firstLine.split(separator)
        .zipWithIndex
        .map { case (_, i) => s"_$i" }
    }
    else
      firstLine.split(separator).map(unescapeString)

    val nField = columns.length

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

    if (firstLines.isEmpty)
      fatal("no data lines in file")

    val sb = new StringBuilder

    val namesAndTypes = {
      if (noImpute) {
        sb.append("Reading table with no type imputation\n")
        columns.map { c =>
          val t = types.getOrElse(c, TString)
          sb.append(s"  Loading column `$c' as type `$t'\n")
          (c, types.getOrElse(c, TString))
        }
      }
      else {
        sb.append(s"Reading table with type imputation from the leading $headToTake lines\n")
        val split = firstLines.tail.map(_.map(_.split(separator)))
        split.foreach { line =>
          line.foreach { fields =>
            if (line.value.length != nField)
              fatal(s"""$firstFile: field number mismatch: header contained $nField fields, found ${line.value.length}""")
          }
        }

        val columnValues = Array.tabulate(nField)(i => split.map(_.value(i)))
        columns.zip(columnValues).map { case (name, col) =>
          types.get(name) match {
            case Some(t) =>
              sb.append(s"  Loading column `$name' as type $t (user-specified)\n")
              (name, t)
            case None =>
              guessType(col, missing) match {
                case Some(t) =>
                  sb.append(s"  Loading column `$name' as type $t (imputed from first $headToTake lines)\n")
                  (name, t)
                case None =>
                  sb.append(s"  Loading column `$name' as type String (no non-missing values in first $headToTake lines)\n")
                  (name, TString)
              }
          }
        }
      }
    }

    info(sb.result())

    val schema = TStruct(namesAndTypes: _*)

    val filter: (String) => Boolean = (line: String) => {
      if (noHeader)
        true
      else line != firstLine
    } && commentChar.forall(ch => !line.startsWith(ch))

    def checkLength(arr: Array[String]) {
      if (arr.length != nField)
        fatal(s"expected $nField fields, but found ${arr.length} fields")
    }


    val rdd = sc.textFilesLines(files)
      .filter(line => filter(line.value))
      .map {
        _.map { line =>
          val split = line.split(separator)
          checkLength(split)
          Annotation.fromSeq(
            (split, namesAndTypes).zipped
              .map { case (elem, (name, t)) =>
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
