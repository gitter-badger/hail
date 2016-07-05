package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.io.annotators._
import org.broadinstitute.hail.utils.{ParseContext, ParseSettings}
import org.broadinstitute.hail.variant.Variant
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

import scala.collection.JavaConverters._

object AnnotateVariantsTable extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "-t", aliases = Array("--types"),
      usage = "Define types of fields in annotations files")
    var types: String = ""

    @Args4jOption(required = true, name = "-r", aliases = Array("--root"),
      usage = "Period-delimited path starting with `va'")
    var root: String = _

    @Args4jOption(required = false, name = "-m", aliases = Array("--missing"),
      usage = "Specify identifier to be treated as missing")
    var missingIdentifier: String = "NA"

    @Args4jOption(required = false, name = "-v", aliases = Array("--vcolumns"),
      usage = "Specify the column identifiers for chromosome, position, ref, and alt (in that order)")
    var vCols: String = "Chromosome,Position,Ref,Alt"

    @Args4jOption(required = false, name = "-s", aliases = Array("--select"),
      usage = "Select only certain columns.  Enter columns to keep as a comma-separated list")
    var select: String = _

    @Args4jOption(required = false, name = "--noheader", usage = "indicate that the file has no header and columns should be indicated by number (0-indexed)")
    var noheader: Boolean = _

    @Args4jOption(required = false, name = "-d", aliases = Array("--delimiter"),
      usage = "Field delimiter regex")
    var separator: String = "\\t"

    @Args4jOption(required = false, name = "-c", aliases = Array("--comment"),
      usage = "Skip lines beginning with the given pattern")
    var commentChar: String = _

    @Argument(usage = "<files...>")
    var arguments: java.util.ArrayList[String] = new java.util.ArrayList[String]()

  }

  def newOptions = new Options

  def name = "annotatevariants table"

  def description = "Annotate variants with TSV file"

  def supportsMultiallelic = false

  def requiresVDS = true

  def parseColumns(s: String): Array[String] = {
    val cols = Parser.parseIdentifierList(s)
    if (cols.length != 4 && cols.length != 1)
      fatal(
        s"""Cannot read chr, pos, ref, alt columns from `$s':
            |  enter four comma-separated column identifiers for separate chr/pos/ref/alt columns, or
            |  one column identifier for a single chr:pos:ref:alt column.""".stripMargin)
    cols
  }

  def run(state: State, options: Options): State = {

    val files = hadoopGlobAll(options.arguments.asScala, state.hadoopConf)
    if (files.isEmpty)
      fatal("Arguments referred to no files")

    val vds = state.vds

    val vCols = parseColumns(options.vCols)
    val keySig =
      if (vCols.length == 1)
        vCols.head -> TVariant
      else
        vCols(1) -> TInt

    val settings = ParseSettings(
      types = Parser.parseAnnotationTypes(options.types) + keySig,
      keyCols = vCols,
      useCols = Option(options.select).map(o => Parser.parseIdentifierList(o)),
      hasHeader = !options.noheader,
      separator = options.separator,
      missing = options.missingIdentifier,
      commentChar = Option(options.commentChar))

    val pc = ParseContext.read(files.head, state.hadoopConf, settings)

    val filter = pc.filter
    val parser = pc.parser

    val annotationTable = state.sc.textFilesLines(files)
      .filter(l => filter(l.value))
      .map(l => l.transform { line =>
        val pl = parser(line.value)
        val v = pl.key match {
          case Array(variant) => variant.asInstanceOf[Variant]
          case Array(chr, pos, ref, alt) => Variant(chr.asInstanceOf[String], pos.asInstanceOf[Int], ref.asInstanceOf[String], alt.asInstanceOf[String])
        }
        (v, pl.value): (Variant, Annotation)
      })

    val annotated = vds
      .withGenotypeStream()
      .annotateVariants(annotationTable, pc.schema,
        Parser.parseAnnotationRoot(options.root, Annotation.VARIANT_HEAD))

    state.copy(vds = annotated)
  }
}
