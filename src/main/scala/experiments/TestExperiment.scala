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

class TestExperiment(ename2:String = "", logN: Int, minD: Int, maxD: Int, qd: Int, file_size: String)(implicit timestampedfolder:String) extends Experiment (s"ubuntuone", ename2, "ubuntuone-expts"){
    // val domain = 1562
    val ls = List(logN, minD, maxD, qd)
    // fileout.println(ls.mkString(","))
    val header="size,method,measure"
    // fileout.println(header)
    def run_TestExperiment(groupSize: Int, version: String, increment_pm: Int, increment_sample: Int, alpha: Double, total_tuples:Long)
    (dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]): Unit = {
        val q = qu.sorted
        val (prepared, pm1) = dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
        var total_cuboid_cost = 0
        prepared.foreach(pm => total_cuboid_cost += 1 << pm.cuboidCost)
        val fetched_cuboids = prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } 
        Profiler.reset(List("Hybrid","Naive","Moment"))
        val (naive_solver, cuboid, mask, pms) = Profiler(s"Prepare") {
            val pm = dc.index.prepareNaive(q).head
            val be = dc.cuboids.head.backend
            val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
            assert(cuboid.n_bits == dc.index.n_bits)
            val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
            val sh = Profiler("Naive") {
                new NaiveSamplingSolver(q.size, cuboid.size.toDouble)
            }
            val pms = dc.index.prepareBatch(q)
            (sh, cuboid, pm.cuboidIntersection, pms)
        }

        cuboid.randomShuffle()  
        //Add cuboids to hybrid solver 

        val pm = dc.index.prepareNaive(q).head
        val be = dc.cuboids.head.backend
        val numWords = ((cuboid.size + 63) >> 6).toInt
        val numGroups = numWords >> groupSize
        var naive_result = naive_solver.solve()
        var naive_errors_max: List[Double] = List()
        var naive_errors_avg: List[Double] = List()
        var naive_times: List[Long] = List()
        println(s"time -1: ${Profiler.getDurationMicro("Naive")}")

        println(s"true result: ${trueResult.toList}")
        /********************** add samples *************************/
        (0 to numGroups).foreach { i =>
            (0 until 1 << groupSize).foreach { j =>
                val s = cuboid.projectFetch64((i << groupSize) + j, mask)
                //add samples to both solvers
                // println(s.toList) 
                Profiler("Naive"){
                    naive_solver.addSample(s)
                }
            } 
                Profiler("Naive"){
                    naive_result = naive_solver.solve()
                }
                
                val (error_Naive_Max,trueValue_Naive,errorAll_Naive,trueResultAll_Naive) = SolverTools.errorMaxWithTrueValue(trueResult, naive_result)
                val result_len_Naive = errorAll_Naive.length
                // println(trueResultAll_Naive)
                val allRatio_Naive = (0 until result_len_Naive).map(i => errorAll_Naive(i) / trueResultAll_Naive.sum)
                val maxRatio_Naive = allRatio_Naive.max
                val avgRatio_Naive = allRatio_Naive.foldLeft(0.0)(_ + _) / allRatio_Naive.length
                
                naive_errors_avg = naive_errors_avg :+ avgRatio_Naive
                naive_errors_max = naive_errors_max :+ maxRatio_Naive
                naive_times = naive_times :+ Profiler.getDurationMicro("Naive")

                if (i < 10) {
                    println(s"step $i: ${naive_result.toList}")
                    println(s"time $i: ${Profiler.getDurationMicro("Naive")}")
                    println(s"err  $i: ${avgRatio_Naive}")
                }

        }
     

        val moment_result = Profiler("Moment") {
            val s = new CoMoment5SolverDouble(q.size, true, null, pm1)
            fetched_cuboids.foreach { case (cols, data) => s.add(cols, data) }
            s.fillMissing()
            s.solve(true) //with heuristics to avoid negative values
        }
        val (error_Moment_Max,trueValue_Moment,errorAll_Moment,trueResultAll_Moment) = SolverTools.errorMaxWithTrueValue(trueResult, moment_result)
        val result_len_Moment = errorAll_Moment.length
        val allRatio_Moment = (0 until result_len_Moment).map(i => errorAll_Moment(i) / trueResultAll_Moment.sum)
        val err = (x: Array[Double],y: Array[Double]) => {
            val temp = x.toList.zip(y.toList).map(z => (z._1 - z._2).abs / y.sum)
            temp.sum / temp.length
        }
        val maxRatio_Moment = allRatio_Moment.max
        val avgRatio_Moment = allRatio_Moment.foldLeft(0.0)(_ + _) / allRatio_Moment.length
        val moment_time = Profiler.getDurationMicro("Moment")


        val str4 = naive_errors_avg.mkString(",")
        val str6 = naive_times.mkString(",")
        
        fileout.println(s"${file_size},online,error,${str4}")
        fileout.println(s"${file_size},online,time,${str6}")
        // fileout.println(s"${file_size},moments,error,${avgRatio_Moment}")
        // fileout.println(s"${file_size},moments,time,${moment_time}")
    }
    
    
    override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
        val increment_pm = 1
        val increment_sample = 6400
        run_TestExperiment(1, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
    }
}

object TestExperiment extends ExperimentRunner {
    def qsize(cg: CubeGenerator, argument: (Int,Int,Int,Int), file_size: String, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
        implicit val be = CBackend.default
        Random.setSeed(1)
        // val (logN, minD, maxD) = (4,4,7)
        // val (logN, minD, maxD, qd) = (8,10,13,8)
        // val (logN, minD, maxD) = (10,10,18)
        // val (logN, minD, maxD) = (15, 18, 30)
        val (logN, minD, maxD, qd) = argument
        // val (logN, minD, maxD, qd) = (9, 10, 30, 10)
        //val (logN, minD, maxD) = (3, 6, 10)
        val sch = cg.schemaInstance
        println(s"schema nbits = ${sch.n_bits}")
        val baseCuboid = cg.loadBase(true)

        val cubename = "ubuntuone"
        val ename = s"${cg.inputname}-$isSMS-qsize"
        val expt = new TestExperiment(ename, logN, minD, maxD, qd, file_size)

        val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
        dc.loadPrimaryMoments(cg.baseName)

        val query_dim = Vector(qd).reverseIterator
        while(query_dim.hasNext)
        {
            var generator_counts = 0
            val qs = query_dim.next()
            while(generator_counts < numIters)
            {
                val query = Tools.generateQuery(isSMS, cg.schemaInstance, qs)
                val prepareNaive = dc.index.prepareNaive(query)
                if (prepareNaive.head.cuboidCost == sch.n_bits) {
                    val trueResult = dc.naive_eval(query)
                    expt.run(dc, dc.cubeName, query, trueResult)
                    generator_counts += 1
                }
                else {
                    println(s"skipping query $query that does not use basecuboid in NaiveSolver")
                }
            }
        }
        be.reset
    }

    def main(args: Array[String]): Unit = {
        implicit val be = CBackend.default
        val argument = (5,10,10,5)
        
        val ubuntuones = List(new UbuntuOne("10M"))
        for (ubuntuone <- ubuntuones){
            def func(param: String)(timestamp: String, numIters: Int) = {
                implicit val ni = numIters
                implicit val ts = timestamp
                param match {
                    case "SMS" => qsize(ubuntuone, argument, ubuntuone.file_size, true)
                    case "RMS" => qsize(ubuntuone, argument, ubuntuone.file_size, false)
                }
            }
            run_expt(func)(args)
        }
    }
}