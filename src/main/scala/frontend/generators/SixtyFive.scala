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
case class SixtyFive()(implicit backend: CBackend) extends CubeGenerator("SixtyFive") {
  override lazy val schemaInstance = schema()

  override val measureName: String = "Amount"
  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val join = (0 until 65).map { i =>
      val num = String.format("%03d", Int.box(i))
    
      val index_value_pair = Iterator((0 to 1)).map(r =>
      schemaInstance.encode_tuple(IndexedSeq(1)) -> 1.toLong)
      //size -> read(n2).map(r => schemaInstance.encode_tuple(r) -> 1L) )
      64->index_value_pair
    }
    join
  }

  override def schema(): StaticSchema2 = {
    val dims = (0 until 1).map(i => LD2[Int](s"Bit_$i", new StaticNatCol(0, 65, defaultToInt, false))).toVector
    val sch = new StaticSchema2(dims)
    sch
  }


}

object SixtyFive {

  def main(args: Array[String]): Unit = {
    // val alpha = 0.1
      val resetSeed = true
      val seedValue = 0L
      val which = "Zeros"
      val arg = args.lift(0).getOrElse("all")
      val params = List{
        (3,1)
      }
      val useBig=true
      val maxD = 2
      // val maxD = 8

      if ((arg equals "base") || (arg equals "all") || (arg equals "two"))  {
        implicit val backend = CBackend.default
        val cg = new SixtyFive()
        if (resetSeed) scala.util.Random.setSeed(seedValue)
        cg.saveBase()
      }

      if ((arg equals "SMS") || (arg equals "all") || (arg equals "two")) {
        implicit val backend = CBackend.default
        val cg = new SixtyFive()
        if (resetSeed) scala.util.Random.setSeed(seedValue)
        params.foreach { case (logN, minD) =>
          cg.saveSMS(logN, minD, maxD)
          backend.reset
        }
      }
  }
}
