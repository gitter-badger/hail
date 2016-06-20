package org.broadinstitute.hail.driver

import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient}
import org.apache.solr.client.solrj.request.schema.SchemaRequest
import org.apache.solr.common.SolrInputDocument
import org.broadinstitute.hail.expr._

import scala.collection.JavaConverters._
import org.broadinstitute.hail.Utils._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable

object ExportVariantsSolr extends Command with Serializable {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-c", aliases = Array("--condition"),
      usage = "comma-separated list of fields/computations to be exported")
    var condition: String = _

    @Args4jOption(name = "-u", aliases = Array("--url"),
      usage = "Solr instance (URL) to connect to")
    var url: String = _

    @Args4jOption(name = "-z", aliases = Array("--zk-host"),
      usage = "Zookeeper host string to connect to")
    var zkHost: String = _

  }

  def newOptions = new Options

  def name = "exportvariantssolr"

  def description = "Export variant information to Solr"

  def supportsMultiallelic = true

  def requiresVDS = true

  def toSolrType(t: Type): String = t match {
    case TInt => "tint"
    case TLong => "tlong"
    case TFloat => "tfloat"
    case TDouble => "tdouble"
    case TBoolean => "boolean"
    case TString => "string"
    // FIXME only 1 deep
    case i: TIterable => toSolrType(i.elementType)
    case _ => fatal("")
  }

  def escapeSolrFieldName(name: String): String = {
    val sb = new StringBuilder

    if (name.head.isDigit)
      sb += '_'

    name.foreach { c =>
      if (c.isLetterOrDigit)
        sb += c
      else
        sb += '_'
    }

    sb.result()
  }

  def solrAddField(solr: SolrClient, name: String, t: Type) {
    val m = mutable.Map.empty[String, AnyRef]

    m += "name" -> escapeSolrFieldName(name)
    m += "type" -> toSolrType(t)
    m += "stored" -> true.asInstanceOf[AnyRef]
    if (t.isInstanceOf[TIterable])
      m += "multiValued" -> true.asInstanceOf[AnyRef]

    val req = new SchemaRequest.AddField(m.asJava)
    req.process(solr)
  }

  def documentAddField(document: SolrInputDocument, name: String, t: Type, value: Any) {
    if (t.isInstanceOf[TIterable]) {
      value.asInstanceOf[Seq[_]].foreach { xi =>
        document.addField(escapeSolrFieldName(name), xi)
      }
    } else
      document.addField(escapeSolrFieldName(name), value)
  }

  def run(state: State, options: Options): State = {
    val sc = state.vds.sparkContext
    val vds = state.vds
    val vas = vds.vaSignature
    val cond = options.condition

    val symTab = Map(
      "v" ->(0, TVariant),
      "va" ->(1, vas))
    val ec = EvalContext(symTab)
    val a = ec.a

    // FIXME use custom parser with constraint on Solr field name
    val parsed = Parser.parseAnnotationArgs(cond, ec)
      .map { case (name, t, f) =>
        assert(name.tail == Nil)
        (name.head, t, f)
      }

    val url = options.url
    val zkHost = options.zkHost

    if (url == null && zkHost == null)
      fatal("one of -u or -z required")

    if (url != null && zkHost != null)
      fatal("both -u and -z given")

    val solr =
      if (url != null)
        new HttpSolrClient(url)
      else
        new CloudSolrClient(zkHost)

    parsed.foreach { case (name, t, f) =>
      solrAddField(solr, name, t)
    }

    vds.sampleIds.foreach { id =>
      solrAddField(solr, id + "_num_alt", TInt)
      solrAddField(solr, id + "_ab", TFloat)
      solrAddField(solr, id + "_gq", TInt)
    }
    
    val sampleIdsBc = sc.broadcast(vds.sampleIds)
    vds.rdd.foreachPartition { it =>
      val ab = mutable.ArrayBuilder.make[AnyRef]
      val documents = it.map { case (v, va, gs) =>

        val document = new SolrInputDocument()

        parsed.foreach { case (name, t, f) =>
          a(0) = v
          a(1) = va

          // FIXME export types
          f().foreach(x => documentAddField(document, name, t, x))
        }

        gs.iterator.zip(sampleIdsBc.value.iterator).foreach { case (g, id) =>
          g.nNonRefAlleles
            .filter(_ > 0)
            .foreach { n =>
              documentAddField(document, id + "_num_alt", TInt, n)
              g.ad.foreach { ada =>
                val ab = ada(0).toDouble / (ada(0) + ada(1))
                documentAddField(document, id + "_ab", TFloat, ab.toFloat)
              }
              g.gq.foreach { gqx =>
                documentAddField(document, id + "_gq", TInt, gqx)
              }
            }
        }

        document
      }

      val solr =
        if (url != null)
          new HttpSolrClient(url)
        else
          new CloudSolrClient(zkHost)

      solr.add(documents.asJava)

      // FIXME back off it commit fails
      solr.commit()
    }

    state
  }
}
