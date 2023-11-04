package experiments

import backend.CBackend
import core.solver.SolverTools
import core.solver.moment.CoMoment5SolverDouble
import core.solver.sampling.NaiveSamplingSolver
import core.{DataCube, PartialDataCube}
import frontend.experiments.Tools
import frontend.generators._
import util.Profiler

import java.io.PrintStream
import scala.util.Random

class ZipfExperiment(ename2:String = "")(implicit timestampedfolder:String) extends Experiment (s"zipf", ename2, "zipf-expts"){
  val header = "CubeName,Query,QSize," +
    "Solver,NumberOfTuples,TotalTime,PrepareTime,FetchTime,SolveTime,Error"
  fileout.println(header)
  val solout = new PrintStream(s"expdata/solution.csv")
  def run_ZipfExperiment(groupSize: Int, version: String, increment_pm: Int, increment_sample: Int, alpha: Double, total_tuples:Long)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo_1 = "NaiveSampling"
    val algo_2 = "MomentSolver"
    val algo_3 = "HybridSolver"
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
    val error_Moment = SolverTools.error(trueResult, moment_result)
    //fileout.println("MomentResult")

    fileout.println(common_2+s"$alpha,$total_cuboid_cost,$totalTime_Moment,$fetchTime_Moment, $solveTime_Moment, $error_Moment")

    val (naive_solver, cuboid, mask, numWords) = Profiler(s"${algo_1}_Prepare") {
      val pm = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits)
      val s = new NaiveSamplingSolver(q.size, cuboid.size.toDouble)
      val numWords = total_cuboid_cost >> 6

      (s, cuboid, pm.cuboidIntersection, numWords)
    }
    fileout.println(numWords)
    val prepareTime_Online = Profiler.getDurationMicro(s"${algo_1}_Prepare")
    cuboid.randomShuffle()
    println(s"\t NaiveSampling Prepare Done")
    Profiler(s"${algo_1}_Fetch") {
      (0 until 1 << numWords).foreach { j =>
        val s = cuboid.projectFetch64(j, mask)
        naive_solver.addSample(s)
      }
    }
    val fetchTime_Online = Profiler.getDurationMicro(s"${algo_1}_Fetch")
    println(s"\t NaiveSampling Fetch Done")
    val online_result = Profiler(s"${algo_1}_Solve") { naive_solver.solve() }
    val solveTime_Online = Profiler.getDurationMicro(s"${algo_1}_Solve")
    val error_Online = SolverTools.error(trueResult, online_result)
    val totalTime_Online = prepareTime_Online + fetchTime_Online + solveTime_Online
    //fileout.println("OnlineResult")

    fileout.println(common_1 + s"$alpha,$totalTime_Online,$prepareTime_Online,$fetchTime_Online,$solveTime_Online,$error_Online")
  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
    val increment_pm = 1
    val increment_sample = 6400

    run_ZipfExperiment(14, "V1", increment_pm, increment_sample,1.1, 1L)(dc, dcname, qu, trueResult)
    //runInterleavingOnline_2(14, "V1", increment_pm, 640, 0.5, total_tuples)(dc, dcname, qu, trueResult)
  }

}

object ZipfExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS:Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    implicit val be = CBackend.default
    Random.setSeed(1024)
    val (logN, minD, maxD) = (15, 10, 30)
    val sch = cg.schemaInstance
    val baseCuboid = cg.loadBase(true)

    val cubename = "Zipf"
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new ZipfExperiment(ename)

    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)

    val query_dim = Vector(18).reverseIterator
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
    val zipf = new ZipfGenerator2()

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
