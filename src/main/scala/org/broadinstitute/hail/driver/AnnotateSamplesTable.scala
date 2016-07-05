package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.{ParseContext, ParseSettings}
import org.kohsuke.args4j.{Option => Args4jOption}

object AnnotateSamplesTable extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-i", aliases = Array("--input"),
      usage = "TSV file path")
    var input: String = _

    @Args4jOption(name = "-k", aliases = Array("--key"),
      usage = "Identify the name of the column containing the sample IDs")
    var keyCol: String = "Sample"

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

    @Args4jOption(required = false, name = "--noheader",
      usage = "indicate that the file has no header and columns should be indicated by number (0-indexed)")
    var noheader: Boolean = _

    @Args4jOption(required = false, name = "-d", aliases = Array("--delimiter"),
      usage = "Field delimiter regex")
    var separator: String = "\\t"

    @Args4jOption(required = false, name = "-c", aliases = Array("--comment"),
      usage = "Skip lines beginning with the given pattern")
    var commentChar: String = _
  }

  def newOptions = new Options

  def name = "annotatesamples table"

  def description = "Annotate samples with a text table"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val vds = state.vds

    val input = options.input

    val settings = ParseSettings(
      types = Parser.parseAnnotationTypes(options.types),
      keyCols = Array(options.keyCol),
      useCols = Option(options.select).map(o => Parser.parseIdentifierList(o)),
      hasHeader = !options.noheader,
      separator = options.separator,
      missing = options.missingIdentifier,
      commentChar = Option(options.commentChar))

    val pc = ParseContext.read(options.input, state.hadoopConf, settings)

    val filter = pc.filter
    val parser = pc.parser

    val m = readLines(options.input, state.hadoopConf) { lines =>
      lines
        .filter(l => filter(l.value))
        .map(l => l.transform { line =>
          val pl = parser(line.value)
          val Array(sample) = pl.key
          (sample.asInstanceOf[String], pl.value): (String, Annotation)
        }).toMap
    }

    val annotated = vds.annotateSamples(m, pc.schema,
      Parser.parseAnnotationRoot(options.root, Annotation.SAMPLE_HEAD))
    state.copy(vds = annotated)
  }
}
