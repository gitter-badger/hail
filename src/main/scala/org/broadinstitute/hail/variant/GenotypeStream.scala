package org.broadinstitute.hail.variant

import java.nio.ByteBuffer

import net.jpountz.lz4.LZ4Factory
import org.apache.spark.sql.types.StructType
import org.broadinstitute.hail.ByteIterator
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr.{TBinary, TInt, TStruct, Type}

import scala.collection.mutable

// FIXME use zipWithIndex
class GenotypeStreamIterator(v: Variant, b: ByteIterator) extends Iterator[Genotype] {
  override def hasNext: Boolean = b.hasNext

  override def next(): Genotype = {
    Genotype.read(v, b)
  }
}

object LZ4Utils {
  val factory = LZ4Factory.fastestInstance()
  val compressor = factory.highCompressor()
  val decompressor = factory.fastDecompressor()

  def compress(a: Array[Byte]): Array[Byte] = {
    val decompLen = a.length

    val maxLen = compressor.maxCompressedLength(decompLen)
    val compressed = Array.ofDim[Byte](maxLen)
    val compressedLen = compressor.compress(a, 0, a.length, compressed, 0, maxLen)

    compressed.take(compressedLen)
  }

  def decompress(decompLen: Int, a: Array[Byte]) = {
    val decomp = Array.ofDim[Byte](decompLen)
    val compLen = decompressor.decompress(a, 0, decomp, 0, decompLen)
    assert(compLen == a.length)

    decomp
  }
}

case class GenotypeStream(variant: Variant, decompLenOption: Option[Int], a: Array[Byte])
  extends Iterable[Genotype] {

  override def iterator: GenotypeStreamIterator = {
    decompLenOption match {
      case Some(decompLen) =>
        new GenotypeStreamIterator(variant, new ByteIterator(LZ4Utils.decompress(decompLen, a)))
      case None =>
        new GenotypeStreamIterator(variant, new ByteIterator(a))
    }
  }

  override def newBuilder: mutable.Builder[Genotype, GenotypeStream] = {
    new GenotypeStreamBuilder(variant)
  }

  def decompressed: GenotypeStream = {
    decompLenOption match {
      case Some(decompLen) =>
        GenotypeStream(variant, None, LZ4Utils.decompress(decompLen, a))
      case None => this
    }
  }

  def compressed: GenotypeStream = {
    decompLenOption match {
      case Some(_) => this
      case None =>
        GenotypeStream(variant, Some(a.length), LZ4Utils.compress(a))
    }
  }

  def toRow: Row = {
    Row.fromSeq(Array(
      decompLenOption.getOrElse(null),
      a
    ))
  }
}

object GenotypeStream {
  def schema: StructType = {
    StructType(Array(
      StructField("decompLen", IntegerType, nullable = true),
      StructField("bytes", BinaryType, nullable = false)
    ))
  }

  def t: Type = TStruct("decompLen" -> TInt,
    "bytes" -> TBinary)

  def fromRow(v: Variant, row: Row): GenotypeStream = {

    val bytes: Array[Byte] = if (row.get(1).isInstanceOf[Array[Byte]]) {
      row.getAs[Array[Byte]](1)
    } else {
      val bb: ByteBuffer = row.getAs[ByteBuffer](1)
      val b: Array[Byte] = Array.ofDim[Byte](bb.remaining())
      bb.get(b)
      b
    }
    GenotypeStream(v,
      row.getAsOption[Int](0),
      bytes)
  }
}

class GenotypeStreamBuilder(variant: Variant, compress: Boolean = true)
  extends mutable.Builder[Genotype, GenotypeStream] {

  val b = new mutable.ArrayBuilder.ofByte

  override def +=(g: Genotype): GenotypeStreamBuilder.this.type = {
    val gb = new GenotypeBuilder(variant)
    gb.set(g)
    gb.write(b)
    this
  }


  def write(gb: GenotypeBuilder) {
    gb.write(b)
  }

  def ++=(i: Iterator[Genotype]): GenotypeStreamBuilder.this.type = {
    i.foreach(this += _)
    this
  }

  override def clear() {
    b.clear()
  }

  override def result(): GenotypeStream = {
    val a = b.result()
    if (compress)
      GenotypeStream(variant, Some(a.length), LZ4Utils.compress(a))
    else
      GenotypeStream(variant, None, a)
  }
}
