package org.broadinstitute.hail.utils

import org.apache.hadoop
import org.apache.spark.sql.Row
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr._

import scala.io.Source

case class ParseSettings(types: Map[String, Type] = Map.empty[String, Type],
  keyCols: Array[String] = Array.empty[String],
  useCols: Option[Seq[String]] = None,
  commentChar: Option[String] = None,
  separator: String = "\t",
  missing: String = "NA",
  hasHeader: Boolean = true)

object ParseContext {

  def read(filename: String,
    hConf: hadoop.conf.Configuration,
    settings: ParseSettings): (ParseContext) = {

    val types = settings.types
    val keyCols = settings.keyCols
    val useCols = settings.useCols
    val commentChar = settings.commentChar
    val sep = settings.separator
    val missing = settings.missing
    val hasHeader = settings.hasHeader

    val keySet = keyCols.toSet

    readFile(filename, hConf) { dis =>
      val lines = Source.fromInputStream(dis)
        .getLines()

      val head = if (lines.isEmpty)
        fatal(
          s"""invalid table file: empty
              |  Offending file: `$filename'
           """.stripMargin)
      else commentChar
        .map(toDrop => lines.dropWhile(_.startsWith(toDrop)))
        .getOrElse(lines)
        .next()

      val columns = if (hasHeader)
        head.split(sep).map(escapeString)
      else head.split(sep)
        .zipWithIndex
        .map { case (_, i) => i.toString }

      def check(toCheck: Iterable[String], msg: String) {
        val colSet = columns.toSet
        toCheck.foreach { col =>
          if (!colSet.contains(col))
            fatal(
              s"""$msg: column `$col' not found.
                 |  Detected columns: [${columns.mkString(",")}]
               """.stripMargin)
        }
      }

      useCols.foreach(included => check(included, "invalid column inclusions"))

      check(keyCols, "invalid key columns")

      check(types.keys, "invalid type mapping")

      val filter: (String) => Boolean = (line: String) => {
        if (hasHeader)
          line != head
        else true
      } && commentChar.forall(ch => !line.startsWith(ch))

      val colIndexMap = columns.zipWithIndex.toMap
      val usedColsAndIndices = columns
        .zipWithIndex
        .filter { case (colName, _) => useCols.map(_.toSet)
          .forall(uc => uc.contains(colName))
        }
        .filter { case (colName, _) => !keySet.contains(colName) }

      if (usedColsAndIndices.isEmpty)
        fatal("no columns used")

      val targetSize = usedColsAndIndices.length + keySet.size
      val fCheck = (arr: Array[String]) =>
        if (arr.length != targetSize)
          fatal(s"expected $targetSize fields, but found ${arr.length} fields")

      def parse(arr: Array[String], name: String, t: Type, i: Int): Any = {
        try {
          val str = arr(i)
          if (str == missing)
            null
          else
            t.asInstanceOf[Parsable].parse(str)
        }
        catch {
          case e: Exception =>
            fatal(s"""${e.getClass.getName}: tried to convert "${arr(i)}" to $t in column "$name" """)
        }
      }

      val keyInfo = keyCols.map(name => (name, types.getOrElse(name, TString), colIndexMap(name)))
      val fKey = (arr: Array[String]) => {
        keyInfo.map { case (name, t, i) => parse(arr, name, t, i) }
      }

      val nonKeyCols = usedColsAndIndices.filter { case (name, i) => !keySet.contains(name) }

      val schema = TStruct({
        usedColsAndIndices.map(_._1)
          .map(colName => (colName, types.getOrElse(colName, TString)))
          .zipWithIndex
          .map { case ((name, t), i) => Field(name, t, i) }
      })

      val parseInfo = usedColsAndIndices.map { case (name, i) => (name, types.getOrElse(name, TString), i) }
      val fValues = (arr: Array[String]) => {
        Row.fromSeq(parseInfo.map { case (name, t, i) => parse(arr, name, t, i) })
      }

      val parseLine: (String) => ParsedLine =
        (line: String) => ParsedLine(line.split(sep), fKey, fValues, fCheck)

      ParseContext(schema, filter, parseLine)
    }
  }
}


case class ParseContext(schema: Type,
  filter: (String) => Boolean,
  parser: (String) => ParsedLine)

case class ParsedLine(private val split: Array[String],
  private val fKey: (Array[String]) => Array[Any],
  private val fVal: (Array[String]) => Row,
  private val check: (Array[String]) => Unit) {

  check(split)

  def key: Array[Any] = fKey(split)

  def value: Row = fVal(split)
}
