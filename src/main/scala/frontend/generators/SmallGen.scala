package frontend.generators

import backend.CBackend
import frontend.schema.encoders.StaticNatCol.defaultToInt
import frontend.schema.encoders.{LazyMemCol, StaticDateCol, StaticNatCol}
import frontend.schema.{LD2, Schema2, StaticSchema2}
import util.BigBinary

import java.util.Date
import scala.collection.immutable.Vector
import scala.io.Source
//Zipf_K100N930kAlpha
//Zipf_D10N100Alpha
case class SmallGen(which: String = "Base")(implicit backend: CBackend) extends CubeGenerator("Zipf_Small_"+which) {
  override lazy val schemaInstance = schema()

  override val measureName: String = "Amount"
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val join = (0 until 1).map { i =>
      val num = String.format("%03d", Int.box(i))
      val n2 = ("Zipf_Small_") + which + ".tbl"
      val size = read(n2).size
      val index_value_pair = read(n2).map(r =>
      schemaInstance.encode_tuple(r.dropRight(1).map(_.toInt)) -> StaticNatCol.defaultToInt(r.last.toDouble.toInt).get.toLong)
      //size -> read(n2).map(r => schemaInstance.encode_tuple(r) -> 1L) )
      size->index_value_pair
    }
    join
  }

  override def schema(): StaticSchema2 = {
    val dims = (0 until 3).map(i => LD2[Int](s"Bit_$i", new StaticNatCol(0, 1, defaultToInt, false))).toVector
    val sch = new StaticSchema2(dims)
    sch
  }

  def read(file: String) = {
    val filename = s"tabledata/"+ ("Zipf_Small") + s"/$file"
    val data = Source.fromFile(filename, "utf-8").getLines().map(_.split("\\|")) //ignore summons_number
    //val data = Source.fromFile(filename, "utf-8").getLines().map(_.split(",")) //ignore summons_number
    data
  }
}

object SmallGen {

  def main(args: Array[String]): Unit = {
    // val alpha = 0.1
      val resetSeed = true
      val seedValue = 0L
      val arg = args.lift(0).getOrElse("all")
      val params = List{
        (3,1)
      }
      val useBig=true
      val maxD = 2
      // val maxD = 8

      if ((arg equals "base") || (arg equals "all") || (arg equals "two"))  {
        implicit val backend = CBackend.default
        val cg = new SmallGen()
        if (resetSeed) scala.util.Random.setSeed(seedValue)
        cg.saveBase()
      }

      if ((arg equals "SMS") || (arg equals "all") || (arg equals "two")) {
        implicit val backend = CBackend.default
        val cg = new SmallGen()
        if (resetSeed) scala.util.Random.setSeed(seedValue)
        params.foreach { case (logN, minD) =>
          cg.saveSMS(logN, minD, maxD)
          backend.reset
        }
      }
  }
}
