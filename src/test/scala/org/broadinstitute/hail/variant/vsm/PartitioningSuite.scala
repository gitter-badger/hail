package org.broadinstitute.hail.variant.vsm

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.check.Prop
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.driver.{Read, Repartition, State, Write}
import org.broadinstitute.hail.variant.{Genotype, Variant, VariantDataset, VariantSampleMatrix}
import org.testng.annotations.Test

class PartitioningSuite extends SparkSuite {

  @Test def testParquetWriteRead() {
    Prop.forAll(VariantSampleMatrix.gen(sc, Genotype.gen(_))) { vds =>
      var state = State(sc, sqlContext, vds)
      state = Repartition.run(state, Array("-n", "4"))
      val out = tmpDir.createTempFile("out", ".vds")
      state = Write.run(state, Array("-o", out))
      val readback = Read.run(state, Array("-i", out))

      println("orig:")
      state.vds.rdd.mapPartitionsWithIndex({ (ind: Int, it: Iterator[(Variant, Annotation, Iterable[Genotype])]) =>
        println(s"partition $ind: ${it.next._1}")
        it
      }).collect()

      println("rb:")
      readback.vds.rdd.mapPartitionsWithIndex({ (ind: Int, it: Iterator[(Variant, Annotation, Iterable[Genotype])]) =>
        println(s"partition $ind: ${it.next._1}")
        it
      }).collect()

      val origFirst = vds.rdd.first()
      val readFirst = readback.vds.rdd.first()

      val p = origFirst == readFirst

      if (!p) {
        println(
          s"""found mismatched partitions:
              |  original: $origFirst
              |  readback: $readFirst
           """.stripMargin)
      }
      p
    }.check()
  }
}
