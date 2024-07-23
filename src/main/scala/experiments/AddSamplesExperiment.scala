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

class AddSamplesExperiment(ename2:String = "", logN: Int, minD: Int, maxD: Int, qd: Int)(implicit timestampedfolder:String) extends Experiment (s"ubuntuone", ename2, "ubuntuone-expts"){
    val domain = 100
    val ls = List(logN, minD, maxD, qd)
    fileout.println(ls.mkString(","))
    val header=(0 to domain).map(x => "hybrid_max_ratio"+x).mkString(",")+","+(0 to domain).map(x => "naive_max_ratio"+x).mkString(",")+","+(0 to domain).map(x => "hybrid_avg_ratio"+x).mkString(",")+","+(0 to domain).map(x => "naive_avg_ratio"+x).mkString(",")+","+"moment_max_ratio,moment_avg_ratio,moment_max_mult,moment_avg_mult,"+(0 to domain).map(x => "hybrid_max_mult"+x).mkString(",")+","+(0 to domain).map(x => "naive_max_mult"+x).mkString(",")+","+(0 to domain).map(x => "hybrid_avg_mult"+x).mkString(",")+","+(0 to domain).map(x => "naive_avg_mult"+x).mkString(",")
    fileout.println(header)
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


        cuboid.randomShuffle()  
        //Add cuboids to hybrid solver 
        fetched_cuboids.foreach { case (bits, array) => hybrid_solver.addCuboid(bits, array) }

        val pm = dc.index.prepareNaive(q).head
        val be = dc.cuboids.head.backend
        val numWords = ((cuboid.size + 63) >> 6).toInt
        val numGroups = numWords >> groupSize
        var hybrid_errors_max: List[Double] = List()
        var naive_errors_max: List[Double] = List()
        var hybrid_errors_avg: List[Double] = List()
        var naive_errors_avg: List[Double] = List()
        var hybrid_mult_max: List[Double] = List()
        var hybrid_mult_avg: List[Double] = List()
        var naive_mult_max: List[Double] = List()
        var naive_mult_avg: List[Double] = List()
        var hybrid_result = hybrid_solver.solve()
        var naive_result = naive_solver.solve()
        (0 to numGroups).foreach { i =>
            (0 until 1 << groupSize).foreach { j =>
                val s = cuboid.projectFetch64((i << groupSize) + j, mask)
                //add samples to both solvers 
                hybrid_solver.addSample(s)
                naive_solver.addSample(s)
            } 
            
             if (i % ((1).max(numGroups / domain)) == 0) {
                    hybrid_result = hybrid_solver.solve() 
                    val (error_Hybrid_Max,trueValue_Hybrid,errorAll_Hybrid,trueResultAll_Hybrid) = SolverTools.errorMaxWithTrueValue(trueResult, hybrid_result)
                    val result_len_Hybrid = errorAll_Hybrid.length
                    val allRatio_Hybrid = (0 until result_len_Hybrid).map(i => errorAll_Hybrid(i) / trueResultAll_Hybrid.sum)
                    val maxRatio_Hybrid = allRatio_Hybrid.max
                    val avgRatio_Hybrid = allRatio_Hybrid.foldLeft(0.0)(_ + _) / allRatio_Hybrid.length
                    val allMult_Hybrid = (0 until result_len_Hybrid).map(i => ( hybrid_result(i) / trueResultAll_Hybrid(i)).max(trueResultAll_Hybrid(i) / hybrid_result(i)))
                    val maxMult_Hybrid = allMult_Hybrid.max
                    val avgMult_Hybrid = allMult_Hybrid.foldLeft(0.0)(_+_) / allMult_Hybrid.length
                    hybrid_errors_avg = hybrid_errors_avg :+ avgRatio_Hybrid
                    hybrid_errors_max = hybrid_errors_max :+ maxRatio_Hybrid
                    hybrid_mult_avg = hybrid_mult_avg :+ avgMult_Hybrid
                    hybrid_mult_max = hybrid_mult_max :+ maxMult_Hybrid
                    naive_result = naive_solver.solve()
                    val (error_Naive_Max,trueValue_Naive,errorAll_Naive,trueResultAll_Naive) = SolverTools.errorMaxWithTrueValue(trueResult, naive_result)
                    val result_len_Naive = errorAll_Naive.length
                    // println(trueResultAll_Naive)
                    val allRatio_Naive = (0 until result_len_Naive).map(i => errorAll_Naive(i) / trueResultAll_Naive.sum)
                    val maxRatio_Naive = allRatio_Naive.max
                    val avgRatio_Naive = allRatio_Naive.foldLeft(0.0)(_ + _) / allRatio_Naive.length
                    val allMult_Naive = (0 until result_len_Naive).map(i => (naive_result(i) / trueResultAll_Naive(i)).max(trueResultAll_Naive(i) / naive_result(i)))
                    val maxMult_Naive = allMult_Naive.max
                    val avgMult_Naive = allMult_Naive.foldLeft(0.0)(_+_) / allMult_Naive.length
                    naive_errors_avg = naive_errors_avg :+ avgRatio_Naive
                    naive_errors_max = naive_errors_max :+ maxRatio_Naive
                    naive_mult_avg = naive_mult_avg :+ avgMult_Naive
                    naive_mult_max = naive_mult_max :+ maxMult_Naive
                    // val naive_index = (allRatio_Naive ++ allPeterNaive).indexOf(maxPeter_Naive)
                    // val culpable_prediction = 
            }
        }

        val s = new CoMoment5SolverDouble(q.size, true, null, pm1)
        fetched_cuboids.foreach { case (cols, data) => s.add(cols, data) }
        s.fillMissing()
        val moment_result=s.solve(true) //with heuristics to avoid negative values
        val (error_Moment_Max,trueValue_Moment,errorAll_Moment,trueResultAll_Moment) = SolverTools.errorMaxWithTrueValue(trueResult, moment_result)
        val result_len_Moment = errorAll_Moment.length
        val allRatio_Moment = (0 until result_len_Moment).map(i => errorAll_Moment(i) / trueResultAll_Moment.sum)
        val err = (x: Array[Double],y: Array[Double]) => {
            val temp = x.toList.zip(y.toList).map(z => (z._1 - z._2).abs / y.sum)
            temp.sum / temp.length
        }
        // println("vvvvvvvvvvvvvvv")
        // println("moment result:")
        // println(moment_result.toList.map(_.toInt) + " " + err(moment_result,trueResult))
        // println("hybrid result:")
        // println(hybrid_result.toList.map(_.toInt) + " " + err(hybrid_result,trueResult))
        // println("online result:")
        // println(naive_result.toList.map(_.toInt) + " " + err(naive_result,trueResult))
        // println("true result:")
        // println(trueResult.toList.map(_.toInt))
        // println("^^^^^^^^^^^^^^")
        val maxRatio_Moment = allRatio_Moment.max
        val avgRatio_Moment = allRatio_Moment.foldLeft(0.0)(_ + _) / allRatio_Moment.length
        val allMult_Moment = (0 until result_len_Moment).map(i => (if(trueResultAll_Moment(i)==0) moment_result(i) / 0.00000001 else moment_result(i) / trueResultAll_Moment(i)).max(if(moment_result(i)==0) trueResultAll_Moment(i) / 0.00000001 else trueResultAll_Moment(i) / moment_result(i)))
        val maxMult_Moment = allMult_Moment.max
        val avgMult_Moment = allMult_Moment.foldLeft(0.0)(_+_) / allMult_Moment.length

        val str = hybrid_errors_max.mkString(",")
        val str2 = naive_errors_max.mkString(",")
        val str3 = hybrid_errors_avg.mkString(",")
        val str4 = naive_errors_avg.mkString(",")
        val str5 = hybrid_mult_max.mkString(",")
        val str6 = naive_mult_max.mkString(",")
        val str7 = hybrid_mult_avg.mkString(",")
        val str8 = naive_mult_avg.mkString(",")
        fileout.println(s"${str},${str2},${str3},${str4},${maxRatio_Moment},${avgRatio_Moment},${maxMult_Moment},${avgMult_Moment},${str5},${str6},${str7},${str8}")
        // fileout.println(s"${maxRatio_Moment},${hybrid_errors(1)},${hybrid_errors(2)}," +
        //   s"${hybrid_errors(3)},${naive_errors(1)},${naive_errors(2)},${naive_errors(3)},${maxPeter_Moment},${hybrid_peter_errors(2)},${naive_peter_errors(2)}")
    }
    
    
    override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
        val increment_pm = 1
        val increment_sample = 6400
        run_TestExperiment(1, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
    }
}

object AddSamplesExperiment extends ExperimentRunner {
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
        val expt = new AddSamplesExperiment(ename, logN, minD, maxD, qd)

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
        val ubuntuone = new UbuntuOne("2M")
        val arguments = List(
            // (4,4,7,2), 
            // (8,10,13,4),
            // (8,10,13,8),
            // (8,10,13,10),
            // (9,10,30,8),
            // (9,10,30,10),
            // (10,10,18,8),
            // (10,10,18,10)
            // (10,10,18,12),
            // (4,4,7,4)
            (9,10,10,4),
            (9,10,10,5),
            (9,10,10,6),
            (9,10,10,7),
            (9,10,10,8),
            (9,10,10,9),
            (9,10,10,10),
            (9,10,10,11),
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