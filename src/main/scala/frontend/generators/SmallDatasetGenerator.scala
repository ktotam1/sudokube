package frontend.generators

import backend.CBackend
import frontend.schema.encoders.StaticNatCol.defaultToInt
import frontend.schema.encoders.{LazyMemCol, StaticDateCol, StaticNatCol}
import frontend.schema.{LD2, Schema2, StaticSchema2}
import util.BigBinary

import java.util.Date
import scala.collection.immutable.Vector
import scala.io.Source

case class SmallDatasetGenerator()(implicit backend: CBackend) extends CubeGenerator("Small_Dataset") {
  override lazy val schemaInstance = schema()

  override val measureName: String = "Amount"
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val join = (0 until 16).map { i =>
      val num = String.format("%03d", Int.box(i))
      val n2 = "small_dataset.part" + num + ".tbl"
      val size = read(n2).size
      val index_value_pair = read(n2).map(r =>
        schemaInstance.encode_tuple(r.dropRight(1).reverse.map(_.toInt)) -> StaticNatCol.defaultToInt(r.last.toDouble.toInt).get.toLong)
      //size -> read(n2).map(r => schemaInstance.encode_tuple(r) -> 1L) )
      size->index_value_pair
    }
    join
  }

  override def schema(): StaticSchema2 = {
    val dims = (1 to 10).map(i => LD2[Int](s"Bit_$i", new StaticNatCol(0, 1, defaultToInt, false))).toVector
    val sch = new StaticSchema2(dims)
    sch
  }

  def read(file: String) = {
    val filename = s"tabledata/SmallDataset/$file"
    val data = Source.fromFile(filename, "utf-8").getLines().map(_.split(",")) //ignore summons_number
    //val data = Source.fromFile(filename, "utf-8").getLines().map(_.split(",")) //ignore summons_number
    data
  }
}

object SmallDatasetGenerator {

  def main(args: Array[String]): Unit = {

    val resetSeed = true //for reproducing the same set of materialization decisions
    val seedValue = 0L

    val arg = args.lift(0).getOrElse("all")
    val params = List(
      //(15, 18),
      //(15, 14), (15, 10), (15, 6),
      //(12, 18), (9, 18), (6, 18)
      //(6, 10), (9, 12), (15, 15)
      (5, 4)
    )
    val maxD = 9

    if ((arg equals "base") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new SmallDatasetGenerator()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      cg.saveBase()
    }

    if ((arg equals "RMS") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new SmallDatasetGenerator()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      params.foreach { case (logN, minD) =>
        cg.saveRMS(logN, minD, maxD)
        backend.reset
      }
    }

    if ((arg equals "SMS") || (arg equals "all")) {
      implicit val backend = CBackend.default
      val cg = new SmallDatasetGenerator()
      if (resetSeed) scala.util.Random.setSeed(seedValue)
      params.foreach { case (logN, minD) =>
        cg.savePreForTenSMS(logN, minD, maxD)
        backend.reset
      }
    }
  }
}
