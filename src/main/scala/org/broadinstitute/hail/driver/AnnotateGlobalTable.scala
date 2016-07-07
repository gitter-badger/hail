package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.{ParseContext, ParseSettings}
import org.kohsuke.args4j.{Option => Args4jOption}

object AnnotateGlobalTable extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-i", aliases = Array("--input"),
      usage = "Path to file")
    var input: String = _

    @Args4jOption(required = false, name = "-t", aliases = Array("--types"),
      usage = "Define types of fields in file")
    var types: String = ""

    @Args4jOption(required = true, name = "-r", aliases = Array("--root"),
      usage = "Argument is a period-delimited path starting with `global'")
    var root: String = _

    @Args4jOption(required = false, name = "-m", aliases = Array("--missing"),
      usage = "Specify identifier to be treated as missing")
    var missingIdentifier: String = "NA"

    @Args4jOption(required = false, name = "-s", aliases = Array("--select"),
      usage = "Select only certain columns.  Enter columns to use as a comma-separated list")
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

  def name = "annotateglobal table"

  def description = "Annotate global table from a text file with multiple columns"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val vds = state.vds

    val path = Parser.parseAnnotationRoot(options.root, Annotation.GLOBAL_HEAD)

    val settings = ParseSettings(
      types = Parser.parseAnnotationTypes(options.types),
      keyCols = Array.empty[String],
      useCols = Option(options.select).map(o => Parser.parseIdentifierList(o)),
      noHeader = options.noHeader,
      separator = options.separator,
      missing = options.missingIdentifier,
      commentChar = Option(options.commentChar))

    val pc = ParseContext.read(options.input, state.hadoopConf, settings)

    val filter = pc.filter
    val parser = pc.parser

    val table = readLines(options.input, state.hadoopConf) { lines => lines
      .filter(l => filter(l.value))
      .map(_.transform { line =>
        val pl = parser(line.value)
        pl.value: Annotation
      })
      .toIndexedSeq
    }

    val (newGlobalSig, inserter) = vds.insertGlobal(TArray(pc.schema), path)

    state.copy(vds = vds.copy(
      globalAnnotation = inserter(vds.globalAnnotation, Some(table)),
      globalSignature = newGlobalSig))
  }
}

