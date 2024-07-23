package experiments


import backend.CBackend
import core.solver.SolverTools
import core.solver.moment.CoMoment5SolverDouble
import core.solver.sampling.{MomentSamplingSolver, NaiveSamplingSolver}
import core.{DataCube, PartialDataCube}
import frontend.experiments.Tools
import frontend.generators._
import util.Profiler

import java.io.PrintStream
import scala.util.Random

class SixtyFiveExperiment(ename2:String = "")(implicit timestampedfolder:String) extends Experiment (s"zipf", ename2, "zipf-expts"){
    val domain = 1

    // fileout.println(header)
    def run_TestExperiment(groupSize: Int, version: String, increment_pm: Int, increment_sample: Int, alpha: Double, total_tuples:Long)
    (dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
        val q = qu.sorted
        val (prepared, pm1) = dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
        var total_cuboid_cost = 0
        prepared.foreach(pm => total_cuboid_cost += 1 << pm.cuboidCost)
        val fetched_cuboids = prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } 
      
        
        val (hybrid_solver, naive_solver, cuboid, mask, pms) = Profiler(s"Prepare") {
            val pm = dc.index.prepareNaive(q).head
            val be = dc.cuboids.head.backend
            val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
            assert(cuboid.n_bits == dc.index.n_bits)
            val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
            val s = new MomentSamplingSolver(q.size, primMoments, version, cuboid.size.toDouble)
            val sh = new NaiveSamplingSolver(q.size, cuboid.size.toDouble)
            val pms = dc.index.prepareBatch(q)
            (s, sh, cuboid, pm.cuboidIntersection, pms)
        }


        
        fetched_cuboids.foreach { case (bits, array) => hybrid_solver.addCuboid(bits, array) }

        val pm = dc.index.prepareNaive(q).head
        val be = dc.cuboids.head.backend
        val numWords = ((cuboid.size + 63) >> 6).toInt
        val numGroups = numWords >> groupSize
        println("-------------------------------------------")

        (0 to numGroups).foreach { i =>
            (0 until 1 << groupSize).foreach { j =>
                val s = cuboid.projectFetch64((i << groupSize) + j, mask)
                //add samples to both solvers 
                hybrid_solver.addSample(s)
                naive_solver.addSample(s)
            
                println("samples s:", s.toList)
                    val hybrid_result = hybrid_solver.solve() 
                    val naive_result = naive_solver.solve()
                    val (error_Naive_Max,trueValue_Naive,errorAll_Naive,trueResultAll_Naive) = SolverTools.errorMaxWithTrueValue(trueResult, naive_result)

                    println(s"true result: ${trueResultAll_Naive.mkString(",")}")
                    println(s"hybrid result: ${hybrid_result.mkString(",")}")
                    println(s"online result: ${naive_result.mkString(",")}")
                    println("-------------------------------------------")
                    
            }
        }

        val s = new CoMoment5SolverDouble(q.size, true, null, pm1)
        fetched_cuboids.foreach { case (cols, data) => s.add(cols, data) }
        s.fillMissing()
        val moment_result=s.solve(true) //with heuristics to avoid negative values
        println(s"moment result: ${moment_result.mkString(",")}")
        println("-------------------------------------------")


        
     
    }
    
    
    override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
        val increment_pm = 1
        val increment_sample = 6400
        run_TestExperiment(1, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
    }
}

object SixtyFiveExperiment extends ExperimentRunner {
    def qsize(cg: CubeGenerator)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
        implicit val be = CBackend.default
        Random.setSeed(1)
        val (logN, minD, maxD) = (3,1,2)

        val sch = cg.schemaInstance
        println(s"schema nbits = ${sch.n_bits}")
        val baseCuboid = cg.loadBase(true)

        val cubename = "Small"
        val isSMS =true
        val ename = s"${cg.inputname}-$isSMS-qsize"
        val expt = new SixtyFiveExperiment(ename)

        val dc = cg.loadSMS(logN, minD, maxD)
        dc.loadPrimaryMoments(cg.baseName)

            val query = Vector(0,1,2,3)
            val prepareNaive = dc.index.prepareNaive(query)
            if (prepareNaive.head.cuboidCost == sch.n_bits) {
                val trueResult = dc.naive_eval(query)
                expt.run(dc, dc.cubeName, query, trueResult)
            }
            else {
                println(s"skipping query $query that does not use basecuboid in NaiveSolver")
            }
            }

    def main(args: Array[String]): Unit = {
        implicit val be = CBackend.default
        val zipf = new SixtyFive()
        def func(param: String)(timestamp: String, numIters: Int) = {
            implicit val ni = numIters
            implicit val ts = timestamp
            qsize(zipf)
        }
        run_expt(func)(args)
    }
}