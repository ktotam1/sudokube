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



case class UbuntuOne(file_size: String)(implicit backend: CBackend) extends CubeGenerator(s"UbuntuOne-${file_size}"){
    override lazy val schemaInstance = schema()

    def schema(): StaticSchema2 = {
        import StaticDateCol._
        def uniq(column: String) = s"tabledata/ubuntu-one-${file_size}/uniq/ubuntuone_trace_${column}.uniq"
        val allFields = List("T", "ext", "Failed", "level", "method", "mime", "msg", "req_t", "time", "tstamp", "type")
        val stringFields = List("T", "ext", "Failed", "level", "method", "mime", "msg", "req_t", "type").map(name => LD2[String](name, new LazyMemCol(uniq(name))))
        val dateFields = List("tstamp").map(name => LD2[Date](name, StaticDateCol.fromFile(uniq(name), simpleDateFormat("yyyy-MM-dd HH:mm:ss"), true, true, true,true,true,true)))
        new StaticSchema2((stringFields ++ dateFields).toVector)
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
      val filename = s"tabledata/ubuntu-one-${file_size}/$file"
      val data: Iterator[Array[String]] =  CSVReader.read(new FileReader(filename), escape='\0').map(x => {if(x.exists(_.contains("u0"))) {
                                                                                                  println(x);
                                                                                                }  
                                                                                                x.toArray}).iterator 
      data
  }
}

object UbuntuOne {
     def main(args: Array[String]): Unit = {

      val resetSeed = true //for reproducing the same set of materialization decisions
      val seedValue = 0L

      val arg = args.lift(0).getOrElse("all")
      val sizes = List("20K", "40M")
      val params = List(
      // (4,4,7),
      // (8,10,13),
      // (9,10,30),
      // (10,10,18),
        (1,10,20)
      )
      for (size <- sizes) {
        if ((arg equals "base") || (arg equals "all")) {
          implicit val backend = CBackend.default
          val cg = new UbuntuOne(size)
          if (resetSeed) scala.util.Random.setSeed(seedValue)
          cg.saveBase()
        }
        if ((arg equals "SMS") || (arg equals "all")) {
          implicit val backend = CBackend.default
          val cg = new UbuntuOne(size)
          if (resetSeed) scala.util.Random.setSeed(seedValue)
          params.foreach { case (logN, minD, maxD) =>
            cg.saveSMS(logN, minD, maxD)
            backend.reset
          }
        }
      }
    }
}