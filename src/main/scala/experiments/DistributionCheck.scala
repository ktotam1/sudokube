package experiments

import backend.CBackend
import core.solver.SolverTools
import core.solver.moment.CoMoment5SolverDouble
import core.solver.sampling.{MomentSamplingSolver, NaiveSamplingSolver}
import core.{DataCube, PartialDataCube}
import frontend.experiments.Tools
import frontend.generators._
import util.Profiler
import frontend.schema.{LD2, Schema2, StaticSchema2}

import java.io.PrintStream
import scala.util.Random
import scala.io.Source

object DistributionCheck {
    def removeFirst[T](list: List[T])(pred: (T) => Boolean): List[T] = {
        val (before, atAndAfter) = list span (x => !pred(x))
        before ::: atAndAfter.drop(1)
      }
    def main(args: Array[String]): Unit = {
        implicit val be = CBackend.default
        val cg = new ZipfGenerator2(2.0, useBig = true)
        val schema: StaticSchema2 = cg.schemaInstance
        val dc = cg.loadBase()
        val cuboid = dc.cuboids(0).asInstanceOf[be.SparseCuboid]
        val groupSize = 1
        var lines: List[List[Int]] = (0 until 1000).map( i => {
            val num = String.format("%03d", Int.box(i))
            val file = "K100N930kAlpha2.0.part"+ num + ".tbl"
            val x: List[List[Int]] = try { 
                val data = Source.fromFile(s"tabledata/"+ "Zipf_K100N930kAlpha" + s"/$file", "utf-8").getLines().map(_.split("\\|")).map(_.map(_.toInt).toList.reverse).toList
                data
            } catch  {
                case e: Exception => List()
            }
            x
        }).foldLeft(List(): List[List[Int]])(_ ::: _)
        // println(lines.take(10).map(r => schema.decode_tuple(schema.encode_tuple(r.tail.reverse.toVector)).map(_._2)).mkString("\n"))
        var cuboidVals: List[Int] = List()
        val numWords = ((cuboid.size + 63) >> 6).toInt
        val numGroups = numWords >> groupSize
         (0 to numGroups).foreach { i =>
            (0 until 1 << groupSize).foreach { j =>
                val vecs = cuboid.fetch64((i << groupSize) + j) 
                cuboidVals = cuboidVals ::: vecs.map(_._2.toInt).toList
            }
        }
        val fileVals = lines.map(_.head)
        println(fileVals.take(10))
        println(cuboidVals.take(10))
        print(fileVals diff cuboidVals)
    }
}