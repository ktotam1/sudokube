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
import util.BitUtils

class SingleTimedAddSamplesExperiment(ename2:String = "", logN: Int, minD: Int, maxD: Int, qd: Int)(implicit timestampedfolder:String) extends Experiment (s"ubuntuone", ename2, "ubuntuone-expts"){
    val domain = 25
    val ls = List(logN, minD, maxD, qd)
    fileout.println(ls.mkString(","))
    val header= "logn,mind,maxd,qd,moment_max_ratio,moment_error,moment_time,num_materialized_cuboids"

    fileout.println(header)
    def run_TestExperiment(groupSize: Int, version: String, increment_pm: Int, increment_sample: Int, alpha: Double, total_tuples:Long)
    (dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
        println(ls)
        val q = qu.sorted
        val (prepared, pm1) = dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
        var total_cuboid_cost = 0
        prepared.foreach(pm => total_cuboid_cost += 1 << pm.cuboidCost)
        val fetched_cuboids = prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } 
        
        Profiler.reset(List("Moment"))
 

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

      
        // println("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
        // fetched_cuboids.foreach{ case (x,y)=>println(BitUtils.IntToSet(x).reverse.toVector)}
        // println(fetched_cuboids.length)
        // println(s"${logN},${maxRatio_Moment},${avgRatio_Moment},${moment_time}")
        // println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        //val header= "logn,mind,maxd,qd,moment_max_ratio,moment_error,moment_time,num_materialized_cuboids"
        fileout.println(s"${logN},${minD},${maxD},${qd},${maxRatio_Moment},${avgRatio_Moment},${moment_time},${fetched_cuboids.length}")
    }
    
    
    override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
        val increment_pm = 1
        val increment_sample = 6400
        run_TestExperiment(1, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
    }
}

object SingleTimedAddSamplesExperiment extends ExperimentRunner {
    def qsize(cg: CubeGenerator, argument: (Int,Int,Int,Int), isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
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
        val expt = new SingleTimedAddSamplesExperiment(ename, logN, minD, maxD, qd)

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
                // val prepareNaive = baseCuboid.index.prepareNaive(query)//dc.index.prepareNaive(query)
                // if (prepareNaive.head.cuboidCost == sch.n_bits) {
                    val trueResult = dc.naive_eval(query)
                    expt.run(dc, dc.cubeName, query, trueResult)
                    generator_counts += 1
                // }
                // else {
                //     println(s"skipping query $query that does not use basecuboid in NaiveSolver")
                // }
            }
        }
        be.reset
    }

    def main(args: Array[String]): Unit = {
        implicit val be = CBackend.default
        val ubuntuone = new UbuntuOne("2M")
        println("starting...")

        val arguments = List(
            (10,10,10,4),
            (10,10,10,4),
            (10,10,10,5),
            (10,10,10,6),
            (10,10,10,7),
            (10,10,10,8),
            (10,10,10,9),
            (10,10,10,10),
            (10,10,10,11)
        )
        for (argument <- arguments){
            def func(param: String)(timestamp: String, numIters: Int) = {
                implicit val ni = numIters
                implicit val ts = timestamp
                param match {
                    case "SMS" => qsize(ubuntuone, argument, true)
                    case "RMS" => qsize(ubuntuone, argument, false)
                }
            }
            run_expt(func)(args)
        }
    }
}