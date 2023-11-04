package frontend.generators

import backend.CBackend
import frontend.schema.encoders.{LazyMemCol, StaticDateCol, StaticNatCol}
import frontend.schema.{LD2, Schema2, StaticSchema2}
import util.BigBinary

import java.util.Date
import scala.collection.immutable.Vector
import scala.io.Source

case class ZipfGenerator2()(implicit backend: CBackend) extends CubeGenerator("Zipf") {
  override lazy val schemaInstance = schema()

  override val measureName: String = "Amount"
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val join = (0 until 1000).map { i =>
      val num = String.format("%03d", Int.box(i))
      val n2 = "K320N93kAlpha1.1.part" + num + ".tbl"
      val size = read(n2).size
      val index_value_pair = read(n2).map(r =>
      schemaInstance.encode_tuple(r.dropRight(1).reverse.map(_.toInt)) -> StaticNatCol.defaultToInt(r.last.toDouble.toInt).get.toLong)
      //size -> read(n2).map(r => schemaInstance.encode_tuple(r) -> 1L) )
      size->index_value_pair
    }
    join
  }

  override def schema(): StaticSchema2 = {

    def unique(i: Int) = s"tabledata/Zipf_2/uniq/K320N93kAlpha1.1.$i.uniq"
    import StaticDateCol._
    val bit1 = LD2[Int]("Bit_1", StaticNatCol.fromFile(unique(20)))
    val bit2 = LD2[Int]("Bit_2", StaticNatCol.fromFile(unique(19)))
    val bit3 = LD2[Int]("Bit_3", StaticNatCol.fromFile(unique(18)))
    val bit4 = LD2[Int]("Bit_4", StaticNatCol.fromFile(unique(17)))
    val bit5 = LD2[Int]("Bit_5", StaticNatCol.fromFile(unique(16)))
    val bit6 = LD2[Int]("Bit_6", StaticNatCol.fromFile(unique(15)))
    val bit7 = LD2[Int]("Bit_7", StaticNatCol.fromFile(unique(14)))
    val bit8 = LD2[Int]("Bit_8", StaticNatCol.fromFile(unique(13)))
    val bit9 = LD2[Int]("Bit_9", StaticNatCol.fromFile(unique(12)))
    val bit10 = LD2[Int]("Bit_10", StaticNatCol.fromFile(unique(11)))
    val bit11 = LD2[Int]("Bit_11", StaticNatCol.fromFile(unique(10)))
    val bit12 = LD2[Int]("Bit_12", StaticNatCol.fromFile(unique(9)))
    val bit13 = LD2[Int]("Bit_13", StaticNatCol.fromFile(unique(8)))
    val bit14 = LD2[Int]("Bit_14", StaticNatCol.fromFile(unique(7)))
    val bit15 = LD2[Int]("Bit_15", StaticNatCol.fromFile(unique(6)))
    val bit16 = LD2[Int]("Bit_16", StaticNatCol.fromFile(unique(5)))
    val bit17 = LD2[Int]("Bit_17", StaticNatCol.fromFile(unique(4)))
    val bit18 = LD2[Int]("Bit_18", StaticNatCol.fromFile(unique(3)))
    val bit19 = LD2[Int]("Bit_19", StaticNatCol.fromFile(unique(2)))
    val bit20 = LD2[Int]("Bit_20", StaticNatCol.fromFile(unique(1)))
    val value_dim = LD2[Int]("Value", StaticNatCol.fromFile(unique(21)))

    val dims1to10 = Vector(bit1, bit2, bit3, bit4, bit5, bit6, bit7, bit8, bit9, bit10)
    val dims11to20 = Vector(bit11, bit12, bit13, bit14, bit15, bit16, bit17, bit18, bit19, bit20)
    val allDims =  Vector(value_dim) ++ dims1to10 ++ dims11to20

    val sch = new StaticSchema2(allDims)
    //sch.columnVector.map(c => s"${c.name} has ${c.encoder.bits.size} bits = ${c.encoder.bits}").foreach(println)
    //println("Total = "+sch.n_bits)
    sch
  }

  def read(file: String) = {
    val filename = s"tabledata/Zipf_2/$file"
    val data = Source.fromFile(filename, "utf-8").getLines().map(_.split("\\|")) //ignore summons_number
    data
  }
}

object ZipfGenerator2 {

  def main(args: Array[String]): Unit = {

    val resetSeed = true //for reproducing the same set of materialization decisions
    val seedValue = 0L

    val arg = args.lift(0).getOrElse("all")
    val params = List(
      //(15, 18),
      //(15, 14), (15, 10), (15, 6),
      //(12, 18), (9, 18), (6, 18)
      //(6, 10), (9, 12), (15, 15)
      (15,10)
    )
    val maxD = 30 //40

    if ((arg equals "base") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new ZipfGenerator2()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      cg.saveBase()
    }

    if ((arg equals "RMS") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new ZipfGenerator2()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      params.foreach { case (logN, minD) =>
        cg.saveRMS(logN, minD, maxD)
        backend.reset
      }
    }

    if ((arg equals "SMS") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new ZipfGenerator2()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      params.foreach { case (logN, minD) =>
        cg.saveSMS(logN, minD, maxD)
        backend.reset
      }
    }

    if ((arg equals "RMSTrie") || (arg equals "all")) {
      //params.foreach { case (logN, minD) =>
      implicit val backend = CBackend.triestore
      val cg = new ZipfGenerator2()
      val dc = cg.loadRMS(15, 18, maxD)
      dc.loadPrimaryMoments(cg.baseName)
      dc.saveAsTrie(20)
      backend.reset
      //}
    }
    if ((arg equals "SMSTrie") || (arg equals "all")) {
      implicit val backend = CBackend.triestore
      val cg = new ZipfGenerator2()
      val dc = cg.loadSMS(15, 18, maxD)
      dc.loadPrimaryMoments(cg.baseName)
      dc.saveAsTrie(20)
      backend.reset
      //}
    }
  }
}
