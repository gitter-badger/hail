package org.broadinstitute.hail.driver

import org.apache.spark.sql.Row
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.{TextTableConfiguration, TextTableReader}
import org.broadinstitute.hail.variant._
import org.json4s.jackson.JsonMethods._
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

import scala.collection.JavaConverters._

object ImportAnnotations extends SuperCommand {
  def name = "importannotations"

  def description = "Import variants and annotations as a sites-only VDS"

  register(ImportAnnotationsTable)
  register(ImportAnnotationsJSON)
}

object ImportAnnotationsTable extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "-t", aliases = Array("--types"),
      usage = "Define types of fields in annotations files")
    var types: String = ""

    @Args4jOption(required = false, name = "-m", aliases = Array("--missing"),
      usage = "Specify identifier to be treated as missing")
    var missingIdentifier: String = "NA"

    @Args4jOption(required = false, name = "-e", aliases = Array("--variant-expr"),
      usage = "Specify the column identifiers for chromosome, position, ref, and alt (in that order)")
    var vExpr: String = "Chromosome,Position,Ref,Alt"

    @Args4jOption(required = false, name = "-s", aliases = Array("--select"),
      usage = "Select only certain columns.  Enter columns to keep as a comma-separated list")
    var select: String = _

    @Args4jOption(required = false, name = "--no-header", usage =
      "indicate that the file has no header and columns should be indicated by `_1, _2, ... _N' (0-indexed)")
    var noHeader: Boolean = _

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

  def name = "importannotations table"

  def description = "Import variants and annotations from a delimited text file as a sites-only VDS"

  def requiresVDS = false

  def supportsMultiallelic = true

  def run(state: State, options: Options): State = {
    val files = hadoopGlobAll(options.arguments.asScala, state.hadoopConf)

    if (files.isEmpty)
      fatal("Arguments referred to no files")
    val settings = TextTableConfiguration(
      types = Parser.parseAnnotationTypes(options.types),
      selection = Option(options.select).map(o => Parser.parseIdentifierList(o)),
      noHeader = options.noHeader,
      separator = options.separator,
      missing = options.missingIdentifier,
      commentChar = Option(options.commentChar))

    val (struct, rdd) = TextTableReader.read(state.sc, files, settings)

    val ec = EvalContext(struct.fields.map(f => (f.name, f.`type`)): _*)
    val variantFn = Parser.parse[Variant](options.vExpr, ec, TVariant)

    val keyedRDD = rdd.map {
      _.map { a =>
        val r = a.asInstanceOf[Row]
        for (i <- 0 until struct.size) {
          ec.set(i, r.get(i))
        }
        variantFn() match {
          case Some(v) => (v, r: Annotation, Iterable.empty[Genotype])
          case None => fatal("invalid variant: missing value")
        }
      }.value
    }

    val vds: VariantDataset = VariantSampleMatrix(VariantMetadata(Array.empty[String], IndexedSeq.empty[Annotation], Annotation.empty,
      TStruct.empty, struct, TStruct.empty), keyedRDD)

    state.copy(vds = vds)
  }

}

object ImportAnnotationsJSON extends Command {

  class Options extends BaseOptions {
    @Argument(usage = "<files...>")
    var arguments: java.util.ArrayList[String] = new java.util.ArrayList[String]()

    @Args4jOption(required = true, name = "-t", aliases = Array("--type"),
      usage = "Type of imported JSON")
    var `type`: String = _

    @Args4jOption(required = true, name = "-e", aliases = Array("--variant-expr"),
      usage = "Expressions for chromosome, position, ref and alt in terms of `root'")
    var variantFields: String = _
  }

  def newOptions = new Options

  def name = "importannotations json"

  def description = "Import variants and annotations from JSON as a sites-only VDS"

  def requiresVDS = false

  def supportsMultiallelic = true

  def run(state: State, options: Options): State = {
    val sc = state.sc

    val files = hadoopGlobAll(options.arguments.asScala, state.hadoopConf)

    if (files.isEmpty)
      fatal("Arguments referred to no files")

    val t = Parser.parseType(options.`type`)

    val extractVariant = JSONAnnotationImpex.jsonExtractVariant(t, options.variantFields)

    val rdd =
      sc.union(files.map { f =>
        sc.textFile(f)
          .map { line =>
            JSONAnnotationImpex.importAnnotation(parse(line), t, "<root>")
          }
      })
        .flatMap { va =>
          extractVariant(va)
            .map { v => (v, va, Iterable.empty[Genotype]) }
        }

    val vds = new VariantDataset(
      VariantMetadata(IndexedSeq.empty, Annotation.emptyIndexedSeq(0), Annotation.empty,
        TStruct.empty, t, TStruct.empty, wasSplit = true),
      rdd)

    state.copy(vds = vds)
  }
}