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

class UbuntuOnePlotterExperiment(ename2:String = "")(implicit timestampedfolder:String) extends Experiment (s"ubunbtuone", ename2, "ubuntuone-expts"){
    val header="moment_result,hybrid_result1/3,hybrid_result2/3,hybrid_result3/3,naive1/3,naive2/3,naive3/3,moment_peter,hybrid_peter2/3,naive_peter2/3"
    fileout.println(header)
    def run_UbuntuOnePlotterExperiment(groupSize: Int, version: String, increment_pm: Int, increment_sample: Int, alpha: Double, total_tuples:Long)
    (dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
        val q = qu.sorted
        val (prepared, pm1) = dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
        var total_cuboid_cost = 0
        prepared.foreach(pm => total_cuboid_cost += 1 << pm.cuboidCost)
        val fetched_cuboids = prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } 
        val s = new CoMoment5SolverDouble(q.size, true, null, pm1)
        fetched_cuboids.foreach { case (cols, data) => s.add(cols, data) }
        s.fillMissing()
        val moment_result = s.solve(true) 
        val (error_Moment_Max,trueValue_Moment,errorAll_Moment,trueResultAll_Moment) = SolverTools.errorMaxWithTrueValue(trueResult, moment_result)
        val result_len_Moment = errorAll_Moment.length
        val allRatio_Moment = (0 until result_len_Moment).map(i =>  if (trueResultAll_Moment(i)==0) 1 else errorAll_Moment(i) / trueResultAll_Moment(i))
        val maxRatio_Moment = allRatio_Moment.max
        val allPeter_Moment = (0 until result_len_Moment).map(i => if (errorAll_Moment(i)==0) 1 else trueResultAll_Moment(i) / errorAll_Moment(i))
        val maxPeter_Moment = (allRatio_Moment ++ allPeter_Moment).max
        

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
        var hybrid_errors: List[Double] = List()
        var naive_errors: List[Double] = List()
        var naive_peter_errors: List[Double] = List()
        var hybrid_peter_errors: List[Double] = List()
        (0 to numGroups).foreach { i =>
            (0 until 1 << groupSize).foreach { j =>
                val s = cuboid.projectFetch64((i << groupSize) + j, mask)
                hybrid_solver.addSample(s)
                naive_solver.addSample(s)
            } 
             if (i % (numGroups / 3) == 0) {
                    val hybrid_result = hybrid_solver.solve() 
                    val (error_Hybrid_Max,trueValue_Hybrid,errorAll_Hybrid,trueResultAll_Hybrid) = SolverTools.errorMaxWithTrueValue(trueResult, hybrid_result)
                    val result_len_Hybrid = errorAll_Hybrid.length
                    val allRatio_Hybrid = (0 until result_len_Hybrid).map(i => if (trueResultAll_Hybrid(i)==0) 1 else errorAll_Hybrid(i) / trueResultAll_Hybrid(i))
                    val maxRatio_Hybrid = allRatio_Hybrid.max
                    hybrid_errors = hybrid_errors :+ maxRatio_Hybrid
                    val allPeter_Hybrid = (0 until result_len_Hybrid).map(i => if(errorAll_Hybrid(i)==0) 1 else trueResultAll_Hybrid(i) / errorAll_Hybrid(i))
                    val maxPeter_Hybrid = (allRatio_Hybrid ++ allPeter_Hybrid).max
                    hybrid_peter_errors = hybrid_peter_errors :+ maxPeter_Hybrid 
                    val naive_result = naive_solver.solve()
                    val (error_Naive_Max,trueValue_Naive,errorAll_Naive,trueResultAll_Naive) = SolverTools.errorMaxWithTrueValue(trueResult, naive_result)
                    val result_len_Naive = errorAll_Naive.length
                    val allRatio_Naive = (0 until result_len_Naive).map(i => if (trueResultAll_Naive(i)==0) 1 else errorAll_Naive(i) / trueResultAll_Naive(i))
                    val maxRatio_Naive = allRatio_Naive.max
                    naive_errors = naive_errors :+ maxRatio_Naive
                    val allPeter_Naive = (0 until result_len_Naive).map(i => if (errorAll_Naive(i) == 0) 1 else trueResultAll_Naive(i) / errorAll_Naive(i))
                    val maxPeter_Naive = (allRatio_Naive ++ allPeter_Naive).max
                    naive_peter_errors = naive_peter_errors :+ maxPeter_Naive
            }
        }
        
        println(s"q: ${q}, moment error: ${maxRatio_Moment}, hybrid error: ${hybrid_errors}, naive error: ${naive_errors}")
        // println(s"moment result: ${moment_result.toList}\n true result: ${trueResult.toList}")
        fileout.println(s"${maxRatio_Moment},${hybrid_errors(1)},${hybrid_errors(2)}," +
          s"${hybrid_errors(3)},${naive_errors(1)},${naive_errors(2)},${naive_errors(3)},${maxPeter_Moment},${hybrid_peter_errors(2)},${naive_peter_errors(2)}")
    }
    
    
    override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
        val increment_pm = 1
        val increment_sample = 6400
        run_UbuntuOnePlotterExperiment(1, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
    }
}

object UbuntuOnePlotterExperiment extends ExperimentRunner {
    def qsize(cg: CubeGenerator, isSMS:Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
        implicit val be = CBackend.default
        Random.setSeed(1)
        val (logN, minD, maxD) = (4, 4, 7)
        //val (logN, minD, maxD) = (3, 6, 10)
        val sch = cg.schemaInstance
        val baseCuboid = cg.loadBase(true)

        val cubename = "ubuntuone"
        val ename = s"${cg.inputname}-$isSMS-qsize"
        val expt = new UbuntuOnePlotterExperiment(ename)

        val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
        dc.loadPrimaryMoments(cg.baseName)

        val query_dim = Vector(3).reverseIterator
        while(query_dim.hasNext)
        {
            var generator_counts = 0
            val qs = query_dim.next()
            println(qs)
            while(generator_counts < numIters)
            {
                val query = Tools.generateQuery(isSMS, cg.schemaInstance, qs)
                println(query)
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
        val ubuntuone = new UbuntuOne("2M")
        def func(param: String)(timestamp: String, numIters: Int) = {
            implicit val ni = numIters
            implicit val ts = timestamp
            param match {
                case "SMS" => qsize(ubuntuone, true)
                case "RMS" => qsize(ubuntuone, false)
            }
        }
        run_expt(func)(args)
    }
}