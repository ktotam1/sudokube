package frontend.generators


import backend.CBackend
import frontend.schema.encoders.{LazyMemCol, StaticDateCol, StaticNatCol}
import frontend.schema.{LD2, Schema2, StaticSchema2}
import util.BigBinary
import java.text.SimpleDateFormat
import breeze.io.{CSVReader, CSVWriter}
import java.io.{File, FileReader, PrintStream}

import java.util.Date
import scala.io.Source



case class UbuntuOneTimeless()(implicit backend: CBackend) extends CubeGenerator("UbuntuOneTimeless"){
    override lazy val schemaInstance = schema()

    def schema(): StaticSchema2 = {
        import StaticDateCol._
        def uniq(column: String) = s"tabledata/ubuntu-one-2M/uniq/ubuntuone_trace_${column}.uniq"
        val allFields = List("T", "ext", "Failed", "level", "method", "mime", "msg", "req_t", "time", "tstamp", "type")
        val stringFields = List("T", "ext", "Failed", "level", "method", "mime", "msg", "req_t", "type").map(name => LD2[String](name, new LazyMemCol(uniq(name))))
        val dateFields = List("tstamp").map(name => LD2[Date](name, StaticDateCol.fromFile(uniq(name), simpleDateFormat("yyyy-MM-dd HH:mm:ss"), true, true, true,true,true,true)))
        new StaticSchema2((stringFields).toVector)
    }
    def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
        val join = (0 until 1000).map { i =>
          val num = String.format("%03d", Int.box(i))
          val n2 = "ubuntuone_trace.part" + num + ".csv"
          val data = read(n2)
          val size = read(n2).size

          size -> data.map(r => {
            schemaInstance.encode_tuple(r) -> 1L
          })
        }
        join
    }
    def read(file: String): Iterator[Array[String]] = {
      val filename = s"tabledata/ubuntu-one-2M/$file"
      val data: Iterator[Array[String]] =  CSVReader.read(new FileReader(filename)).map(_.toArray).iterator 
      data
  }
}

object UbuntuOneTimeless {
     def main(args: Array[String]): Unit = {

      val resetSeed = true //for reproducing the same set of materialization decisions
      val seedValue = 0L

      val arg = args.lift(0).getOrElse("all")
      val params = List(
      (12,15)
      )
      val maxD = 30

      if ((arg equals "base") || (arg equals "all")) {
        implicit val backend = CBackend.default
        val cg = new UbuntuOneTimeless()
        if (resetSeed) scala.util.Random.setSeed(seedValue)
        cg.saveBase()
      }
      if ((arg equals "SMS") || (arg equals "all")) {
        implicit val backend = CBackend.default
        val cg = new UbuntuOneTimeless()
        if (resetSeed) scala.util.Random.setSeed(seedValue)
        params.foreach { case (logN, minD) =>
          cg.saveSMS(logN, minD, maxD)
          backend.reset
        }
      }
    }
}