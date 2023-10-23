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
class InterleavingOnlineSampling(ename2: String = "")(implicit timestampedfolder:String) extends Experiment(s"interleaving", ename2, "online-sampling"){

  val header = "CubeName,Query,QSize," +
    "Solver,FractionOfSamples,TotalTime,PrepareTime,FetchTime,SolveTime,Error"
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
      val pms_iterator = dc.index.prepareOnline(q, 2).iterator
      val numWords = ((cuboid.size + 63) >> 6).toInt
      val numGroups = numWords >> groupSize
      val pi = new ProgressIndicator(numGroups, "\t Sampling")
      (s, cuboid, pm.cuboidIntersection, pms_iterator,numWords,numGroups,pi )
    }
    println(s"\t  Prepare Done")
    val prepareTime = Profiler.getDurationMicro("Prepare")

    var initResult:Array[Double] = Array.fill(trueResult.size)(0.0)
    val (fetched_pms, fetched_samples) = Profiler("Fetch") {

      val fetched_pms = pms.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm))}
      val fetched_samples_tmp:ArrayBuffer[Array[Long]] = null
      for(i <- 1 to numGroups)
      {
        for(j <- 1 to 1 << groupSize)
        {
          fetched_samples_tmp += cuboid.projectFetch64((i << groupSize) + j, mask)
        }
        pi.step
      }


      val fetched_samples = fetched_samples_tmp.toIterator
      (fetched_pms, fetched_samples)
    }
    //val samples_iterator = (1 to numGroups).flatMap(i=> (1 to groupSize).map(j => (i, j))).iterator
    //val fetched_sample = cuboid.projectFetch64((samples_iterator.next()._1 << groupSize) + samples_iterator.next()._2, mask)
    println(s"\t  Fetch Done")
    var pm_iter_times: Int = 0
    var sample_iter_times: Int = 0
    var flag: Int = 1
    while((pm_iter_times < pms.length) || (sample_iter_times < numGroups))
    {
      if((pm_iter_times < pms.length) && (sample_iter_times < numGroups))
      {
        if(flag == 1)
        {
          initResult = if (version == "V1") {
            Profiler("Solve") {
              for(i <- 1 to increment_pm)
              {
                if(fetched_pms.hasNext)
                {
                  val current_pm = fetched_pms.next()
                  solver.addCuboid(current_pm._1, current_pm._2)
                }
                else
                {
                  break
                }
              }
              solver.solve()
            }
          } else {
            Array.fill(trueResult.size)(0.0)
          }
          pm_iter_times = pm_iter_times + increment_pm
          flag = 0
        }
        else
        {
          initResult = if (version == "V1") {
            Profiler("Solve") {
              for(i <- 1 to increment_sample)
              {
                if(fetched_samples.hasNext)
                {
                  val current_sample = fetched_samples.next()
                  solver.addSample(current_sample)
                }
                else
                {
                  break
                }
              }
              solver.solve()
            }
          }
          else {
            Array.fill(trueResult.size)(0.0)
          }
          sample_iter_times = sample_iter_times + increment_sample
          flag = 1
        }
      }
      else if((pm_iter_times < pms.length) && (sample_iter_times == numGroups))
      {
        initResult = if (version == "V1") {
          Profiler("Solve") {
            for (i <- 1 to increment_pm)
            {
              if(fetched_pms.hasNext)
              {
                val current_pm = fetched_pms.next()
                solver.addCuboid(current_pm._1, current_pm._2)
              }
              else{
                break
              }
            }
            solver.solve()
          }

        } else {
          Array.fill(trueResult.size)(0.0)
        }
        pm_iter_times = pm_iter_times + increment_pm
      }
      else
      {
        initResult = if (version == "V1") {
          Profiler("Solve") {
            for (i <- 1 to increment_sample) {
              if (fetched_samples.hasNext) {
                val current_sample = fetched_samples.next()
                solver.addSample(current_sample)
              }
              else
              {
                break
              }
            }
            solver.solve()
          }
        }
        else {
          Array.fill(trueResult.size)(0.0)
        }
        sample_iter_times = sample_iter_times + increment_sample
      }
      val initError = SolverTools.error(trueResult, initResult)

      def fetchTime = Profiler.getDurationMicro("Fetch")

      def solveTime = Profiler.getDurationMicro("Solve")

      def totalTime = prepareTime + fetchTime + solveTime
      fileout.println(common + s"0.0,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")

    }

    val result = Profiler("Solve") {
      solver.solve()
    }
    val error = SolverTools.error(trueResult, result)
    val fraction = (sample_iter_times + 1).toDouble / (numGroups + 1)

    def fetchTime = Profiler.getDurationMicro("Fetch")

    def solveTime = Profiler.getDurationMicro("Solve")

    def totalTime = prepareTime + fetchTime + solveTime
    fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$error")

  }

  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
    val increment_pm = 1
    val increment_sample = 1
    runInterleavingOnline(14, "V1", increment_pm, increment_sample)(dc, dcname, qu, trueResult)

  }
}

object InterleavingOnlineSampling extends ExperimentRunner {
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
    val expt = new InterleavingOnlineSampling(ename)
    //val mqr = new MaterializedQueryResult(cg, isSMS)  //for loading pre-generated queries and results
    Vector(10, 12, 14, 18).reverse.map{
      qs =>

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