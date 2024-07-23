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

class ZipfExperiment(ename2:String = "")(implicit timestampedfolder:String) extends Experiment (s"zipf", ename2, "zipf-expts"){
  //val header = "CubeName,Query,QSize," + "Solver,Fraction,Alpha,TotalTime,PrepareTime,FetchTime,SolveTime,Error,TrueValue,ErrorMax,Max_True_Ratio,ErrorMaxOrigin,MaxRatio"
  val header = "CubeName,Query,QSize," +
    "Solver,Fraction,Alpha,Error,Result_pred_max,Result_true_max,MaxRatio"
  fileout.println(header)
  val solout = new PrintStream(s"expdata/solution.csv")
  def run_ZipfExperiment(groupSize: Int, version: String, increment_pm: Int, increment_sample: Int, alpha: Double, total_tuples:Long)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo_1 = "NaiveSampling"
    val algo_2 = "MomentSolver"
    val algo_3 = "NaiveSamplingTo50"
    val q = qu.sorted
    val common_1 = s"$dcname,${qu.mkString(":")},${qu.size},$algo_1,"
    val common_2 = s"$dcname,${qu.mkString(":")},${qu.size},$algo_2,"
    val common_3 = s"$dcname,${qu.mkString(":")},${qu.size},$algo_3,"
    Profiler.resetAll()
    fileout.println(dc.index.n_bits)
    val (prepared, pm) = Profiler(s"${algo_2}_Prepare") {
      dc.index.prepareBatch(q) -> SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
    }
    println(s"\t Moment Prepare Done")
    val prepareTime_Moment = Profiler.getDurationMicro(s"${algo_2}_Prepare")
    var initResult: Array[Double] = Array.fill(trueResult.size)(0.0)
    var total_cuboid_cost = 0
    val fetched_cuboids = Profiler(s"${algo_2}_Fetch") {
      prepared.foreach(pm => total_cuboid_cost += 1 << pm.cuboidCost)
      prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } 
    }
    println(s"\t Moment Fetch Done.")
    val fetchTime_Moment = Profiler.getDurationMicro(s"${algo_2}_Fetch")
    val moment_result = Profiler(s"${algo_2}_Solve") {
      val s = new CoMoment5SolverDouble(q.size, true, null, pm)
      fetched_cuboids.foreach { case (cols, data) => s.add(cols, data) }
      s.fillMissing()
      s.solve(true) //with heuristics to avoid negative values
    }
    println(s"\t Moment Solve Done")
    val solveTime_Moment = Profiler.getDurationMicro(s"${algo_2}_Solve")
    val totalTime_Moment = prepareTime_Moment + fetchTime_Moment + solveTime_Moment
    val (error_Moment_Max,trueValue_Moment,errorAll_Moment,trueResultAll_Moment) = SolverTools.errorMaxWithTrueValue(trueResult, moment_result)
    println(s"error all moment average: ${errorAll_Moment.foldLeft(0.0){_+_} / errorAll_Moment.length}")
    val err_ratio_Moment = error_Moment_Max.toDouble / trueValue_Moment
    val error_Moment = SolverTools.error(trueResult, moment_result)
    val error_Moment_maximum = SolverTools.errorMax(trueResult, moment_result)
    fileout.println("MomentResult") // commented before
    val result_len_Moment = errorAll_Moment.length
    val allRatio_Moment = (0 until result_len_Moment).map(i => errorAll_Moment(i) / trueResultAll_Moment(i))
    val errorAll_Moment_res = errorAll_Moment.mkString(",")
    val trueResultAll_Moment_res = trueResultAll_Moment.mkString(",")
    val MaxRatio_Moment = allRatio_Moment.max
    val allRatio_Moment_res = allRatio_Moment.mkString(",")
    val moment_res = moment_result.mkString(",")
    fileout.println(s"$errorAll_Moment_res")  //commented before
    fileout.println(s"$moment_res") //commented before
    fileout.println(s"$trueResultAll_Moment_res")  //commented before
    val (MaxRatio_Moment, maxIndex_m) = allRatio_Moment.zipWithIndex.maxBy { case (value, _) => value }
    val related_true_res_m = trueResultAll_Moment(maxIndex_m)
    val related_pred_res_m = errorAll_Moment(maxIndex_m)
    fileout.println(s"$allRatio_Moment_res") //commented before as well as vvvvv
    fileout.println(common_2+s"0.0,$alpha,$total_cuboid_cost,$totalTime_Moment,$fetchTime_Moment,$solveTime_Moment,$error_Moment,$trueValue_Moment,$error_Moment_Max,$err_ratio_Moment,$error_Moment_maximum,$MaxRatio_Moment")
    fileout.println(common_2+s"0.0,$alpha,$error_Moment,$related_pred_res_m,$related_true_res_m,$MaxRatio_Moment") //commented before as well as ^^^^^^
    val (naive_solver, cuboid, mask, numWords, numTotalWords, numGroups) = Profiler(s"${algo_1}_Prepare") {
      val pm = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits)
      //val s = new NaiveSamplingSolver(q.size, cuboid.size.toDouble)
      val s = new NaiveSamplingSolver(q.size, cuboid.size.toDouble)
      val numWords = (total_cuboid_cost+63) >> 6
      val numTotalWords = ((cuboid.size + 63) >> 6).toInt
      val numGroups = numWords >> groupSize
      (s, cuboid, pm.cuboidIntersection, numWords, numTotalWords, numGroups)
    }

    val prepareTime_Online = Profiler.getDurationMicro(s"${algo_1}_Prepare")
    cuboid.randomShuffle()
    println(s"\t NaiveSampling Prepare Done")

    (0 to numGroups).foreach { i =>
      Profiler(s"${algo_1}_Fetch") {
        (0 until 1 << groupSize).foreach { j =>
          val s = cuboid.projectFetch64((i << groupSize) + j, mask)
          naive_solver.addSample(s)
        }
      }
    }
    val fetchTime_Online = Profiler.getDurationMicro(s"${algo_1}_Fetch")
    println(s"\t NaiveSampling Fetch Done")
    val fraction = numWords.toDouble / numTotalWords.toDouble
    val online_result = Profiler(s"${algo_1}_Solve") { naive_solver.solve() }
    println(s"\t NaiveSampling Solve Done")
    val solveTime_Online = Profiler.getDurationMicro(s"${algo_1}_Solve")
    val (error_Online_Max, trueValue_Online,errorAll_Online, trueResultAll_Online) = SolverTools.errorMaxWithTrueValue(trueResult, online_result)
    val err_ratio_Online = error_Online_Max.toDouble / trueValue_Online
    val error_Online = SolverTools.error(trueResult, online_result)
    val error_Online_maximum = SolverTools.errorMax(trueResult, online_result)
    val totalTime_Online = prepareTime_Online + fetchTime_Online + solveTime_Online
    fileout.println("OnlineResult") //commented before
    val result_len_Online = errorAll_Online.length
    val allRatio_Online = (0 until result_len_Online).map(i => errorAll_Online(i) / trueResultAll_Online(i))
    val errorAll_Online_res = errorAll_Online.mkString(",")
    val trueResultAll_Online_res = trueResultAll_Online.mkString(",")
    val allRatio_Online_res = allRatio_Online.mkString(",")
    val online_res = online_result.mkString(",")
    val MaxRatio_Online = allRatio_Online.max
    val (MaxRatio_Online, maxIndex) = allRatio_Online.zipWithIndex.maxBy{case(value, _)=>value}
    val related_true_res = trueResultAll_Online(maxIndex)
    val related_pred_res = errorAll_Online(maxIndex)
    fileout.println(s"$online_res")

    fileout.println(s"$trueResultAll_Online_res")
    fileout.println(s"$errorAll_Online_res")
    fileout.println(s"$allRatio_Online_res")
    fileout.println(common_1 + s"$fraction,$alpha,$error_Online,$related_pred_res,$related_true_res,$MaxRatio_Online")

     

    val (naive_solver50, cuboid50, mask50, numWords50, numGroups50) = Profiler(s"${algo_3}_Prepare") {
      val pm50 = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid50 = dc.cuboids(pm50.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid50.n_bits == dc.index.n_bits)
      val s50 = new NaiveSamplingSolver(q.size, cuboid50.size.toDouble)
      val numWords50 = ((cuboid50.size + 63) >> 6).toInt
      val numGroups50 = numWords50 >> groupSize

      (s50, cuboid50, pm50.cuboidIntersection, numWords50, numGroups50)
    }
    val num_total_samples = cuboid50.size.toDouble
    var num_input_samples = 0
    (0 until numGroups50).foreach { i =>
      if(num_input_samples < num_total_samples - (1<<groupSize)*64) {
        Profiler(s"${algo_3}_Fetch") {
          (0 until 1 << groupSize).foreach { j =>
            val s = cuboid50.projectFetch64((i << groupSize) + j, mask50)
            naive_solver50.addSample(s)
            fileout.println(s(0).toString)
            fileout.println(s(1).toString)
          }
        }
        num_input_samples += (1<<groupSize)*64
      }
      else{
        val num_left_samples = (num_total_samples - num_input_samples).toInt
        Profiler(s"${algo_3}_Fetch") {
          (0 until num_left_samples/64).foreach { j =>
            val s = cuboid50.projectFetch64((i << groupSize) + j, mask50)
            naive_solver50.addSample(s)
          }
        }
        num_input_samples += num_left_samples
      }

      val Online50_result = Profiler(s"${algo_3}_Solve") {
        naive_solver50.solve()
      }
      val prepareTime_Online50 = Profiler.getDurationMicro(s"${algo_3}_Prepare")
      val fetchTime_Online50 = Profiler.getDurationMicro(s"${algo_3}_Fetch")
      val solveTime_Online50 = Profiler.getDurationMicro(s"${algo_3}_Solve")
      val totalTime_Online50 = prepareTime_Online50 + fetchTime_Online50 + solveTime_Online50
      val (error_Online50_Max, trueValue_Online50,errorAll_Online50, trueResultAll_Online50) = SolverTools.errorMaxWithTrueValue(trueResult, Online50_result)
      val err_ratio_Online50 = error_Online50_Max.toDouble / trueValue_Online50
      val error_Online50 = SolverTools.error(trueResult, Online50_result)
      val error_Online50_maximum = SolverTools.errorMax(trueResult, Online50_result)

      val fraction = num_input_samples / num_total_samples
      val result_len_Online50 = errorAll_Online50.length
      val allRatio_Online50 = (0 until result_len_Online50).map(i => errorAll_Online50(i) / trueResultAll_Online50(i))
      val allRatio_Online50_str = allRatio_Online50.mkString(":")
      /*
      val (MaxRatio_Online, maxIndex) = allRatio_Online.zipWithIndex.maxBy{case(value, _)=>value}
          val related_true_res = trueResultAll_Online(maxIndex)
          val related_pred_res = errorAll_Online(maxIndex)
       */
      val (maxRatio_Online50, maxIndex_Online50) = allRatio_Online50.zipWithIndex.maxBy{case(value, _)=>value}
      val related_true_res_Online50 = trueResultAll_Online50(maxIndex_Online50)
      val related_pred_res_Online50 = errorAll_Online50(maxIndex_Online50)
      //fileout.println(common_3 + s"$fraction,$alpha,$error_Online50,$related_pred_res_Online50,$related_true_res_Online50,$maxRatio_Online50")
      if(fraction==1)
        {
          //fileout.println(allRatio_Online50_str)
        }
    }

    val (hybrid_solver, hybrid_cuboid, hybrid_mask, hybrid_pms, hybird_numWords, hybrid_numGroups) = Profiler(s"Prepare_Hybrid") {
      val pm = dc.index.prepareNaive(q).head // get prematerialized base cuboid
      val be = dc.cuboids.head.backend //get backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits) //number of basemoment cuboid
      val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val pms_iterator = dc.index.prepareBatch(q).iterator
      //val pms_iter = dc.index.prepareBatch(q)
      val numWords = ((cuboid.size + 63) >> 6).toInt
      val numGroups = numWords >> groupSize
      val s = new MomentSamplingSolver(q.size, primMoments, version, (numGroups + 1) * (1 << groupSize) * 64)
      //total number of samples is (numGroups+1) * (1 << groupSize) * 64
      (s, cuboid, pm.cuboidIntersection, pms_iterator, numWords, numGroups)
    }
    hybrid_cuboid.randomShuffle() //random shuffle
    println(s"\t  Hybrid Solver Prepare Done")
    val prepareTime_Hybrid = Profiler.getDurationMicro("Prepare_Hybrid")

    var initResult_Hybrid: Array[Double] = Array.fill(trueResult.size)(0.0)
    val samples_iterator = (0 to hybrid_numGroups).flatMap(i => (0 until (1 << groupSize)).map(j => (i, j))).iterator


  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
    val increment_pm = 1
    val increment_sample = 6400
    //run_ZipfExperiment(0, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
    run_ZipfExperiment(1, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
  }

}

object ZipfExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS:Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    implicit val be = CBackend.default
    Random.setSeed(1)
    val (logN, minD, maxD) = (9, 10, 30)
    //val (logN, minD, maxD) = (3, 6, 10)
    val sch = cg.schemaInstance
    val baseCuboid = cg.loadBase(true)

    val cubename = "Zipf"
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new ZipfExperiment(ename)

    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)

    val query_dim = Vector(10).reverseIterator
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
    val zipf = new ZipfGenerator2(2.0)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {
        case "zipf-prefix" => qsize(zipf, true)
        case "zipf-random" => qsize(zipf, false)
      }
    } 

    run_expt(func)(args)
  }
}
