package experiments

import backend.CBackend
import core.{DataCube, MaterializedQueryResult}
import core.solver.SolverTools
import core.solver.iterativeProportionalFittingSolver.{MSTVanillaIPFSolver, VanillaIPFSolver}
import core.solver.moment.CoMoment5SolverDouble
import core.solver.sampling.{IPFSamplingSolver, MomentSamplingSolver, NaiveSamplingSolver}
import frontend.experiments.Tools
import frontend.generators._
import util.{Profiler, ProgressIndicator}

import java.io.{File, PrintStream}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
class InterleaveOnlineExperiment(ename2: String = "")(implicit timestampedfolder:String) extends Experiment(s"interleaving", ename2, "online-sampling"){

  val header = "CubeName,Query,QSize," +
    "Solver,FractionOfSamples,ElapsedTotalTime,PrepareTime,FetchTime,SolveTime,Error"
  fileout.println(header)
  val solout = new PrintStream(s"expdata/solution.csv")

  /**
  In this method, we define another two input argument:
  @param increment_pm:Int, the number of pms we feed to the solver once
  @param increment_sample:Int, the number of samples we feed to the solver once
   */
  def runInterleavingOnline(groupSize: Int, version: String, increment_pm:Int, increment_sample:Int)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = s"MomentOnline-$version-$groupSize"
    val q = qu.sorted
    val common = s"$dcname,${qu.mkString(":")},${qu.size},$algo,"
    Profiler.resetAll()
    println(s"Running $algo")

    val (solver, cuboid, mask, pms, numWords,numGroups, pi) = Profiler(s"Prepare") {
      val pm = dc.index.prepareNaive(q).head
      val be = dc.cuboids.head.backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits) //number of basemoment cuboid
      val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val s = new MomentSamplingSolver(q.size, primMoments, version, cuboid.size.toDouble)
      val pms_iterator = dc.index.prepareBatch(q).iterator
      val numWords = ((cuboid.size + 63) >> 6).toInt
      val numGroups = numWords >> groupSize
      val pi = new ProgressIndicator(numGroups, "\t Sampling")
      (s, cuboid, pm.cuboidIntersection, pms_iterator,numWords,numGroups,pi )
    }
    println(s"\t  Prepare Done")
    val prepareTime = Profiler.getDurationMicro("Prepare")

    var initResult:Array[Double] = Array.fill(trueResult.size)(0.0)
    val samples_iterator = (0 to numGroups).flatMap(i=> (0 to  1 << groupSize).map(j => (i, j))).iterator
    //val samples_iterator = (1 to numGroups).iterator
    //val fetched_sample = cuboid.projectFetch64((samples_iterator.next()._1 << groupSize) + samples_iterator.next()._2, mask)
    var flag: Int = 1
    var sample_num = 0

    while(samples_iterator.hasNext || pms.hasNext) {
      println("Start Loop")
      flag = -flag
      if(flag == 1) {
        val fetched_pms = Profiler("Fetch") {
          Iterator.fill(increment_pm) {
            if (!pms.hasNext) {
              break
            }
            val current_pm = pms.next()
            (current_pm.queryIntersection, dc.fetch2[Double](List(current_pm)))
          }
        }
        initResult = if (version == "V1") {
          Profiler("Solve") {
            fetched_pms.foreach(current_pm =>
            solver.addCuboid(current_pm._1, current_pm._2))
            solver.solve()
          }
        }
        else{
          Array.fill(trueResult.size)(0.0)
        }

      }
      else {
        val fetched_samples = Profiler("Fetch"){
          Iterator.fill(increment_pm) {
            if (!samples_iterator.hasNext) {
              break
            }
            val current_sample = samples_iterator.next()
            /*
            (0 until 1 << groupSize).foreach { j =>
              val s = cuboid.projectFetch64((i << groupSize) + j, mask)
              solver.addSample(s)
            }
            */

            val fetched_sample = cuboid.projectFetch64((current_sample._1 << groupSize) + current_sample._2, mask)
            fetched_sample
          }
        }
        initResult = if (version == "V1") {
          Profiler("Solve") {
            fetched_samples.foreach(current_sample =>
              solver.addSample(current_sample))
            solver.solve()
          }
        }
        else {
          Array.fill(trueResult.size)(0.0)
        }
      }
      if(flag > 0)
        {

        }
      else if(samples_iterator.hasNext)
        {
          sample_num  = sample_num + increment_sample
        }
      else{
        sample_num = 1 << groupSize * (numGroups + 1)
      }
      //val pm = pms.next()
      //val fetched_pm = (pm.queryIntersection, dc.fetch2[Double](List(pm)))
      val initError = SolverTools.error(trueResult, initResult)

      def fetchTime = Profiler.getDurationMicro("Fetch")

      def solveTime = Profiler.getDurationMicro("Solve")

      def totalTime = prepareTime + fetchTime + solveTime

      val fraction = sample_num.toDouble / ((numGroups + 1) * (1 << groupSize))
      fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")
    }



    val result = Profiler("Solve") {
      solver.solve()
    }
    val error = SolverTools.error(trueResult, result)
    //val fraction = (sample_iter_times + 1).toDouble / (numGroups + 1)

    def fetchTime = Profiler.getDurationMicro("Fetch")

    def solveTime = Profiler.getDurationMicro("Solve")

    def totalTime = prepareTime + fetchTime + solveTime
    fileout.println(common + s"1.0,$totalTime,$prepareTime,$fetchTime,$solveTime,$error") //$fraction,

  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
    val increment_pm = 5
    val increment_sample = 10
    runInterleavingOnline(14, "V1", increment_pm, increment_sample)(dc, dcname, qu, trueResult)

  }
}

object InterleaveOnlineExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    val logN = 9
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val sch = cg.schemaInstance
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new InterleaveOnlineExperiment(ename)
    //val mqr = new MaterializedQueryResult(cg, isSMS)  //for loading pre-generated queries and results
    val query_dim = Vector(10, 12, 14, 16, 18, 20, 22, 24).reverseIterator
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

    /*
    Vector(10, 12, 14, 18).reverse.map { qs =>
      val queries = (0 until numIters).map //needs to be changed to meet the condition,generate up to 100, not just filter
      { i => Tools.generateQuery(isSMS, cg.schemaInstance, qs) } //generate fresh queries
      //val queries = mqr.loadQueries(qs).take(numIters)
      queries.zipWithIndex.foreach { case (q, qidx) =>
        val preparedNaive = dc.index.prepareNaive(q)
        if (preparedNaive.head.cuboidCost == sch.n_bits) //check whether cuboid is basecuboid:last cuboid in datacube, when cuboidcost equals to the total number of dim, it is the basecuboid
        {
          val trueResult = dc.naive_eval(q)
          //val trueResult = mqr.loadQueryResult(qs, qidx)
          expt.run(dc, dc.cubeName, q, trueResult)
        }
        //otherwise, w
        else {
          println(s"skipping query $q that does not use basecuboid in NaiveSolver")
        }
      }
    }

     */

    be.reset
  }
  def main(args: Array[String]): Unit = {
    implicit val be = CBackend.default
    val ssb = new SSB(1)

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {
        case "qsize-ssb-prefix" => qsize(ssb, true)
        case "qsize-ssb-random" => qsize(ssb, false)
      }
    }

    run_expt(func)(args)
  }
}