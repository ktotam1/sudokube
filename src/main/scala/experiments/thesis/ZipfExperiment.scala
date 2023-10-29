package experiments

import backend.CBackend
import core.materialization.PresetMaterializationStrategy
import core.{DataCube, MaterializedQueryResult, PartialDataCube}
import core.solver.SolverTools
import core.solver.moment.CoMoment5SolverDouble
import core.solver.sampling.{MomentSamplingSolver, NaiveSamplingSolver}
import frontend.generators._
import frontend.experiments.Tools
import util.{Profiler, ProgressIndicator}

import scala.util.Random
import java.io.{File, PrintStream}

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
    fileout.println("MomentResult")
    fileout.println(common_2+s"$alpha,$total_cuboid_cost,$totalTime_Moment,$fetchTime_Moment, $solveTime_Moment")

    val (naive_solver, cuboid, mask, numWords, numGroups) = Profiler(s"${algo_1}_Prepare") {
      val pm = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits)
      val s = new NaiveSamplingSolver(q.size, cuboid.size.toDouble)
      val numWords = total_cuboid_cost >> 6
      val numGroups = numWords >> groupSize
      (s, cuboid, pm.cuboidIntersection, numWords, numGroups)
    }
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
    fileout.println("OnlineResult")
    fileout.println(common_1 + s"$alpha,$totalTime_Online,$prepareTime_Online,$fetchTime_Online,$solveTime_Online,$error_Online")
  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
    val increment_pm = 1
    val increment_sample = 6400
    val alpha = 1.0
    val total_tuples = 1L << 17
    run_ZipfExperiment(14, "V1", increment_pm, increment_sample,1, 1L)(dc, dcname, qu, trueResult)
    //runInterleavingOnline_2(14, "V1", increment_pm, 640, 0.5, total_tuples)(dc, dcname, qu, trueResult)
  }

}

object ZipfExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS:Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    implicit val be = CBackend.default
    val sch = cg.schemaInstance
    val baseCuboid = cg.loadBase(true)
    val cubename = "zipf"
    val ename = "zipf_experiment"
    val expt = new ZipfExperiment()(ename)
    val mstrat = PresetMaterializationStrategy(sch.n_bits, Vector(
      Vector(1, 6, 9, 14, 16),
      Vector(0, 3, 5, 8, 12),
      Vector(5, 9, 12, 14, 17),
      Vector(0, 9, 11, 15, 17),
      Vector(0, 2, 6, 7, 11, 13),
      Vector(6, 9, 10, 12, 13, 14),
      Vector(1, 2, 4, 5, 8, 17),
      Vector(4, 6, 7, 9, 14, 15),
      Vector(0, 2, 6, 10, 13, 16, 17),
      Vector(5, 6, 10, 12, 14, 15, 17),
      Vector(4, 7, 8, 11, 12, 13, 15, 17),
      Vector(3, 6, 8, 9, 11, 12, 13, 15),
      Vector(6, 7, 10, 11, 12, 13, 14, 16),
      Vector(2, 4, 7, 8, 9, 10, 13, 14, 15, 16),
      Vector(1, 2, 3, 4, 6, 7, 11, 12, 13, 15),
      Vector(8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
      Vector(2, 3, 4, 5, 8, 9, 10, 11, 12, 14, 13, 16),
      Vector(9, 12, 13, 17),
      Vector(4, 6, 7, 11),
      Vector(6, 12, 15, 16),
      Vector(2, 5, 9, 14),
      (0 until sch.n_bits) //base cuboid must be always included at last position
    ))

    val dataCube = new PartialDataCube(cubename, cg.baseName)
    dataCube.buildPartial(mstrat)
    dataCube.save()
    dataCube.primaryMoments = SolverTools.primaryMoments(dataCube)
    val dc = PartialDataCube.load(cubename, cg.baseName)
    dc.loadPrimaryMoments(cg.baseName)


    //val mqr = new MaterializedQueryResult(cg, isSMS)  //for loading pre-generated queries and results
    //val query_dim = Vector(10, 12, 14, 18).reverseIterator
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
    val zipf = new ZipfGenerator()

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
