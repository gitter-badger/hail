package org.broadinstitute.hail.variant

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.methods.{CovariateData, Pedigree, LoadVCF}
import org.testng.annotations.Test

class HardCallSetSuite extends SparkSuite {
  @Test def test() {
    val d = DenseCallStream.DenseCallStreamFromGtStream(Iterable(0,1,2,3), 4)

    println(d)

    d.showBinary()

    val vds = LoadVCF(sc, "src/test/resources/linearRegression.vcf")
    //val ped = Pedigree.read("src/test/resources/linearRegression.fam", sc.hadoopConfiguration, vds.sampleIds)
    //val cov = CovariateData.read("src/test/resources/linearRegression.cov", sc.hadoopConfiguration, vds.sampleIds)

    val e = HardCallSet(vds)
    println(e.sampleIds)
    e.rdd.foreach(println)

    e.write(sqlContext, "/tmp/hardcalls.hcs")
    val f = HardCallSet.read(sqlContext, "/tmp/hardcalls.hcs")

    println(f.sampleIds)
    def toComp(hcs: HardCallSet) = hcs.rdd.mapValues(_.a.toList).collect().toSet
    assert(toComp(e) == toComp(f))
  }


}