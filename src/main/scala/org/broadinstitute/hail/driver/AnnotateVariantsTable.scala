package org.broadinstitute.hail.driver

import org.apache.spark.sql.Row
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.{TextTableConfiguration, TextTableReader}
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

    @Args4jOption(required = false, name = "-e", aliases = Array("--variant-expr"),
      usage = "Specify an expression to construct a variant from the fields of the text table")
    var vExpr: String = "Variant(Chromosome,Position,Ref,Alt)"

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

    @Argument(usage = "<files...>")
    var arguments: java.util.ArrayList[String] = new java.util.ArrayList[String]()
  }

  def newOptions = new Options

  def name = "annotatevariants table"

  def description = "Annotate variants with delimited text file"

  def supportsMultiallelic = false

  def requiresVDS = true

  def run(state: State, options: Options): State = {

    val files = hadoopGlobAll(options.arguments.asScala, state.hadoopConf)
    if (files.isEmpty)
      fatal("Arguments referred to no files")

    val vds = state.vds

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
          case Some(v) => (v, r: Annotation)
          case None => fatal("invalid variant: missing value")
        }
      }.value
    }

    val annotated = vds
      .withGenotypeStream()
      .annotateVariants(keyedRDD, struct,
        Parser.parseAnnotationRoot(options.root, Annotation.VARIANT_HEAD))

    state.copy(vds = annotated)
  }
}
