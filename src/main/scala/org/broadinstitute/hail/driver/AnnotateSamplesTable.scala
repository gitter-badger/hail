package org.broadinstitute.hail.driver

import org.apache.spark.sql.Row
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.{TextTableConfiguration, TextTableReader}
import org.kohsuke.args4j.{Option => Args4jOption}

object AnnotateSamplesTable extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-i", aliases = Array("--input"),
      usage = "TSV file path")
    var input: String = _

    @Args4jOption(required = true, name = "-e", aliases = Array("--sample-expr"),
      usage = "Expression of columns to form sample ID")
    var sampleExpr: String = _

    @Args4jOption(required = false, name = "-t", aliases = Array("--types"),
      usage = "Define types of fields in annotations files")
    var types: String = ""

    @Args4jOption(required = true, name = "-r", aliases = Array("--root"),
      usage = "Argument is a period-delimited path starting with `sa'")
    var root: String = _

    @Args4jOption(required = false, name = "-m", aliases = Array("--missing"),
      usage = "Specify identifier to be treated as missing")
    var missingIdentifier: String = "NA"

    @Args4jOption(required = false, name = "-s", aliases = Array("--select"),
      usage = "Select only certain columns.  Enter columns to keep as a comma-separated list")
    var select: String = _

    @Args4jOption(required = false, name = "--no-header",
      usage = "indicate that the file has no header and columns should be indicated by `_1, _2, ... _N' (0-indexed)")
    var noHeader: Boolean = _

    @Args4jOption(required = false, name = "-d", aliases = Array("--delimiter"),
      usage = "Field delimiter regex")
    var separator: String = "\\t"

    @Args4jOption(required = false, name = "-c", aliases = Array("--comment"),
      usage = "Skip lines beginning with the given pattern")
    var commentChar: String = _
  }

  def newOptions = new Options

  def name = "annotatesamples table"

  def description = "Annotate samples with a delimited text file"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val vds = state.vds

    val input = options.input

    val settings = TextTableConfiguration(
      types = Parser.parseAnnotationTypes(options.types),
      noHeader = options.noHeader,
      separator = options.separator,
      missing = options.missingIdentifier,
      commentChar = Option(options.commentChar))

    val (struct, rdd) = TextTableReader.read(state.sc, Array(options.input), settings)

    val (filteredStruct, filterer) = Option(options.select)
      .map(Parser.parseIdentifierList)
      .map { cols =>

        val fields = struct.fields.map(_.name).toSet
        cols.foreach { name =>
          if (!fields.contains(name))
            fatal(s"""invalid column selection `$name'""")
        }
        val selectionSet = cols.toSet
        struct.filter((f: Field) => selectionSet.contains(f.name))
      }
      .getOrElse(struct.filter(_ => true))

    val ec = EvalContext(struct.fields.map(f => (f.name, f.`type`)): _*)
    val sampleFn = Parser.parse[String](options.sampleExpr, ec, TString)

    val map = rdd
      .map {
        _.map { a =>
          val r = a.asInstanceOf[Row]
          for (i <- 0 until struct.size) {
            ec.set(i, r.get(i))
          }
          sampleFn() match {
            case Some(s) => (s, filterer(r))
            case None => fatal("invalid sample: missing value")
          }
        }.value
      }
      .collect()
      .toMap

    val annotated = vds.annotateSamples(map, filteredStruct,
      Parser.parseAnnotationRoot(options.root, Annotation.SAMPLE_HEAD))
    state.copy(vds = annotated)
  }
}
