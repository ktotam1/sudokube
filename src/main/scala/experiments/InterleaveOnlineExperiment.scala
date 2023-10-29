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
import scala.util.Random
import java.io.{File, PrintStream}
import scala.collection.mutable.ArrayBuffer
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
  def runInterleavingOnline_1(groupSize: Int, version: String, increment_pm:Int, increment_sample:Int, alpha:Double)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = s"MomentOnline-$version-$groupSize"
    val q = qu.sorted
    val common = s"$dcname,${qu.mkString(":")},${qu.size},$algo,"
    Profiler.resetAll()
    println(s"Running $algo")

    val (solver, cuboid, mask, pms, numWords, numGroups, pi) = Profiler(s"Prepare") {
      val pm = dc.index.prepareNaive(q).head // get prematerialized base cuboid
      val be = dc.cuboids.head.backend //get backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits) //number of basemoment cuboid
      cuboid.randomShuffle() //random shuffle
      val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)

      val pms_iterator = dc.index.prepareBatch(q).iterator
      //val pms_iter = dc.index.prepareBatch(q)
      val numWords = ((cuboid.size + 63) >> 6).toInt
      val numGroups = numWords >> groupSize
      val pi = new ProgressIndicator(numGroups, "\t Sampling")
      val s = new MomentSamplingSolver(q.size, primMoments, version, (numGroups + 1) * (1 << groupSize) * 64)
      //total number of samples is (numGroups+1) * (1 << groupSize) * 64
      (s, cuboid, pm.cuboidIntersection, pms_iterator, numWords, numGroups, pi)
    }
    println(s"\t  Prepare Done")
    val prepareTime = Profiler.getDurationMicro("Prepare")

    var initResult: Array[Double] = Array.fill(trueResult.size)(0.0)
    val samples_iterator = (0 to numGroups).flatMap(i => (0 until (1 << groupSize)).map(j => (i, j))).iterator
    //val samples_iterator = (1 to numGroups).flatMap(i => (1 to (1 << groupSize)).map(j => (i, j))).iterator
    //val samples_iterator = (1 to numGroups).iterator
    //val fetched_sample = cuboid.projectFetch64((samples_iterator.next()._1 << groupSize) + samples_iterator.next()._2, mask)
    var flag: Int = 1
    var sample_num = 0

    var all_samples_finished = 0 //record whether all the samples are fetched by the solver
    var all_cuboids_finished = 0 //record whether all the cuboids are fetched by the solver
    var fetched_cuboid_size: Option[Long] = None
    while (samples_iterator.hasNext || pms.hasNext) {
      if (flag == 1) {
        if (all_cuboids_finished == 0) {
          val fetched_pms = Profiler("Fetch") {
            val take_pm = pms.take(increment_pm)
            val pm_list = take_pm.toList
            val pm_iter = pm_list.iterator
            Iterator.fill(pm_list.length) {
              val current_pm = pm_iter.next()
              val current_cuboidCost:Int = current_pm.cuboidCost
              val current_cuboidId:Int = current_pm.cuboidID
              fetched_cuboid_size = Some(1 << current_cuboidCost)
              fileout.println(s"cuboidId is $current_cuboidId, cuboidCost is $current_cuboidCost")
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
          else {
            Array.fill(trueResult.size)(0.0)
          }
          if (pms.isEmpty) {
            all_cuboids_finished = 1 //change flag and make sure we will never go back to fetch cuboids
            fileout.println("All cuboids are added to the solver.")
          }
          val initError = SolverTools.error(trueResult, initResult)

          def fetchTime = Profiler.getDurationMicro("Fetch")

          def solveTime = Profiler.getDurationMicro("Solve")

          def totalTime =  fetchTime + solveTime

          //def totalTime = prepareTime + fetchTime + solveTime

          val sampleDouble = sample_num.toDouble
          val fraction = sample_num.toDouble / ((numGroups + 1) * (1 << groupSize))
          fileout.println(s"Cuboid_Size = $fetched_cuboid_size")
          fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")
        }
        flag = -flag
      }

      else {
        if (all_samples_finished == 0) {
          //fetched_cuboid_size: the dimension of cuboid(cuboidCost)
          //get the equal number of tuples in samples
          //if we fetch the cuboid, input samples with the same number tuples that this cuboid has
          val num_sample_equal_cuboid_1: Long = fetched_cuboid_size.getOrElse(increment_sample*64) //Long: prepare for higher dimension
          var num_sample_equal_cuboid:Double = num_sample_equal_cuboid_1 * alpha //multiplied by alpha
          num_sample_equal_cuboid = num_sample_equal_cuboid / 64 //fetch64 get 64 tuples once
          var samples_once = 0 //one assistant variable that contains the number of samples we add once
          //Some questions: The dimension size of the cuboids( <= 32 ? <= 64?)
          val fetched_samples = Profiler("Fetch") {
            var sample_list:List[(Int, Int)] = List()
            while(num_sample_equal_cuboid > increment_sample)
              {
                val take_sample = samples_iterator.take(increment_sample).toList
                num_sample_equal_cuboid = num_sample_equal_cuboid - increment_sample
                sample_list = sample_list ::: take_sample
              }
            val take_sample = samples_iterator.take(num_sample_equal_cuboid.toInt + 1).toList
            sample_list = sample_list ::: take_sample
            val sample_iter =  sample_list.toIterator
            //val take_sample = samples_iterator.take(increment_sample)
            //val sample_list = take_sample.toList
            //val sample_iter = sample_list.iterator
            //val sample_iter = samples_list.iterator
            samples_once = sample_list.length //
            Iterator.fill(sample_list.length) {
              val current_sample = sample_iter.next()
              val s = cuboid.projectFetch64((current_sample._1 << groupSize) + current_sample._2, mask)
              s
            }
          }
          initResult = if (version == "V1") {
            Profiler("Solve") {
              fetched_samples.foreach(current_sample =>
                solver.addSample(current_sample))
              //solver.solve()
              solver.solve()
            }
          }
          else {
            Array.fill(trueResult.size)(0.0)
          }
          if (samples_iterator.isEmpty) {
            all_samples_finished = 1 //change flag and make sure we will never go back to fetch cuboids
            //fileout.println("All samples are added to the solver.")
          }
          if (samples_iterator.hasNext) {
            //sample_num = sample_num + increment_sample
            sample_num = sample_num + samples_once
          }
          else {
            sample_num = (1 << groupSize) * (numGroups + 1)
          }
          val initError = SolverTools.error(trueResult, initResult)

          def fetchTime = Profiler.getDurationMicro("Fetch")

          def solveTime = Profiler.getDurationMicro("Solve")

          def totalTime = fetchTime + solveTime

          //def totalTime = prepareTime + fetchTime + solveTime

          val fraction = sample_num.toDouble / ((numGroups + 1) * (1 << groupSize))
          val sampleDouble = sample_num.toDouble
          //fileout.println(s"Sample_num = $sample_num")
          fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")
        }
        flag = -flag
        fetched_cuboid_size = None
      }
    }
  }

  //TODO:Implement the second method
  def runInterleavingOnline_2(groupSize: Int, version: String, increment_pm: Int, increment_sample: Int, alpha: Double, total_tuples:Long)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) = {
    val algo = s"MomentOnline-$version-$groupSize"
    val q = qu.sorted
    val common = s"$dcname,${qu.mkString(":")},${qu.size},$algo,"
    Profiler.resetAll()
    println(s"Running $algo")

    val (solver, cuboid, mask, pms, numWords, numGroups, pi) = Profiler(s"Prepare") {
      val pm = dc.index.prepareNaive(q).head // get prematerialized base cuboid
      val be = dc.cuboids.head.backend //get backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits) //number of basemoment cuboid
      cuboid.randomShuffle() //random shuffle
      val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)

      val pms_iterator = dc.index.prepareBatch(q).iterator
      //val pms_iter = dc.index.prepareBatch(q)
      val numWords = ((cuboid.size + 63) >> 6).toInt
      val numGroups = numWords >> groupSize
      val pi = new ProgressIndicator(numGroups, "\t Sampling")
      val s = new MomentSamplingSolver(q.size, primMoments, version, (numGroups + 1) * (1 << groupSize) * 64)
      //total number of samples is (numGroups+1) * (1 << groupSize) * 64
      (s, cuboid, pm.cuboidIntersection, pms_iterator, numWords, numGroups, pi)
    }
    println(s"\t  Prepare Done")
    val prepareTime = Profiler.getDurationMicro("Prepare")

    var initResult: Array[Double] = Array.fill(trueResult.size)(0.0)
    val samples_iterator = (0 to numGroups).flatMap(i => (0 until (1 << groupSize)).map(j => (i, j))).iterator
    //val samples_iterator = (1 to numGroups).flatMap(i => (1 to (1 << groupSize)).map(j => (i, j))).iterator
    //val samples_iterator = (1 to numGroups).iterator
    //val fetched_sample = cuboid.projectFetch64((samples_iterator.next()._1 << groupSize) + samples_iterator.next()._2, mask)
    var flag: Int = 1
    var sample_num = 0

    var all_samples_finished = 0 //record whether all the samples are fetched by the solver
    var all_cuboids_finished = 0 //record whether all the cuboids are fetched by the solver
    //var fetched_cuboid_size: Option[Long] = None
    val tuples_for_cuboids = alpha * total_tuples // the tuples number we set for cuboids
    var fetched_tuples_for_cuboids = 0L // the actual number of fetched tuples
    while (samples_iterator.hasNext || pms.hasNext) {
      if (flag == 1) {
        if (all_cuboids_finished == 0) {
          val fetched_pms = Profiler("Fetch") {
            var fetched_pms_list: List[(Int, Array[Double])] = Nil
            while(fetched_tuples_for_cuboids < tuples_for_cuboids && pms.hasNext)
              {
                if(pms.hasNext)
                {
                  val current_pm = pms.next()
                  val current_cuboidCost: Int = current_pm.cuboidCost
                  val current_cuboidId: Int = current_pm.cuboidID
                  fetched_tuples_for_cuboids = fetched_tuples_for_cuboids + (1L << current_cuboidCost)
                  fileout.println(s"cuboidCost = $current_cuboidCost")
                  fetched_pms_list = fetched_pms_list :+ (current_pm.queryIntersection, dc.fetch2[Double](List(current_pm)))
                }
              }
              fetched_pms_list
            }
          /*
          initResult = if (version == "V1") {
            Profiler("Solve") {
              fetched_pms.foreach(current_pm =>
                solver.addCuboid(current_pm._1, current_pm._2))
              solver.solve()
            }
          }

           */
          if (version == "V1") {
            Profiler("Solve_1") {
              fetched_pms.foreach(current_pm =>
                solver.addCuboid(current_pm._1, current_pm._2))
              //solver.solve()
            }
            Profiler("Solve_2") {
              solver.solve()
            }
          }
          else {
            Array.fill(trueResult.size)(0.0)
          }
          if (pms.isEmpty) {
            all_cuboids_finished = 1
            fileout.println("All cuboids are added to the solver.")
          }
          val initError = SolverTools.error(trueResult, initResult)

          def fetchTime = Profiler.getDurationMicro("Fetch")

          def solveTime_1 = Profiler.getDurationMicro("Solve_1")
          def solveTime_2 = Profiler.getDurationMicro("Solve_2")
          def solveTime = solveTime_1 + solveTime_2

          def totalTime = fetchTime + solveTime

          //def totalTime = prepareTime + fetchTime + solveTime

          val sampleDouble = sample_num.toDouble
          val fraction = sample_num.toDouble / ((numGroups + 1) * (1 << groupSize))

          fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")
        }
        flag = -flag
        val ns: Seq[String] = Seq("Solve_2")
        Profiler.reset(ns)
      }

      else {
        if (all_samples_finished == 0) {
          //fetched_cuboid_size: the dimension of cuboid(cuboidCost)
          //get the equal number of tuples in samples
          //if we fetch the cuboid, input samples with the same number tuples that this cuboid has
          var num_sample_equal_cuboid: Double = fetched_tuples_for_cuboids * (1 - alpha) / alpha //multiplied by alpha
          if(num_sample_equal_cuboid <= 0)
            {
              num_sample_equal_cuboid = increment_sample * 64
            }
          num_sample_equal_cuboid = num_sample_equal_cuboid / 64 //devided by 64: fetch64 once receives 64 tuples
          var samples_once = 0 //one assistant variable that contains the number of samples we add once
          //Some questions: The dimension size of the cuboids( <= 32 ? <= 64?)
          val fetched_samples = Profiler("Fetch") {
            var sample_list: List[(Int, Int)] = List()
            while (num_sample_equal_cuboid > increment_sample) {
              val take_sample = samples_iterator.take(increment_sample).toList
              num_sample_equal_cuboid = num_sample_equal_cuboid - increment_sample
              sample_list = sample_list ::: take_sample
            }
            val take_sample = samples_iterator.take(num_sample_equal_cuboid.toInt + 1).toList
            sample_list = sample_list ::: take_sample
            val sample_iter = sample_list.toIterator

            samples_once = sample_list.length
            Iterator.fill(sample_list.length) {
              val current_sample = sample_iter.next()
              val s = cuboid.projectFetch64((current_sample._1 << groupSize) + current_sample._2, mask)
              s
            }
          }
          initResult = if (version == "V1") {
            Profiler("Solve_1") {
              fetched_samples.foreach(current_sample =>
                solver.addSample(current_sample))
              //solver.solve()
            }
            Profiler("Solve_2") {
              solver.solve()
            }
          }
          else {
            Array.fill(trueResult.size)(0.0)
          }
          if (samples_iterator.isEmpty) {
            all_samples_finished = 1
            //fileout.println("All samples are added to the solver.")
          }
          if (samples_iterator.hasNext) {
            //sample_num = sample_num + increment_sample
            sample_num = sample_num + samples_once
          }
          else {
            sample_num = (1 << groupSize) * (numGroups + 1)
          }
          val initError = SolverTools.error(trueResult, initResult)

          def fetchTime = Profiler.getDurationMicro("Fetch")

          def solveTime_1 = Profiler.getDurationMicro("Solve_1")

          def solveTime_2 = Profiler.getDurationMicro("Solve_2")

          def solveTime = solveTime_1 + solveTime_2

          def totalTime =  fetchTime + solveTime

          //def totalTime = prepareTime + fetchTime + solveTime

          val fraction = sample_num.toDouble / ((numGroups + 1) * (1 << groupSize))
          val sampleDouble = sample_num.toDouble
          //fileout.println(s"Sample_num = $sample_num")
          fileout.println(common + s"$fraction,$totalTime,$prepareTime,$fetchTime,$solveTime,$initError")
        }
        flag = -flag
        fetched_tuples_for_cuboids = 0 //clear the fetch_cuboid_size, and wait for next update
        val ns: Seq[String] = Seq("Solve_2")
        Profiler.reset(ns)
      }
    }
  }
  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
    val increment_pm = 1
    val increment_sample = 6400
    val alpha = 1.0
    val total_tuples = 1L << 17
    runInterleavingOnline_1(14, "V1", increment_pm, increment_sample, 1)(dc, dcname, qu, trueResult)
    //runInterleavingOnline_2(14, "V1", increment_pm, 640, 0.5, total_tuples)(dc, dcname, qu, trueResult)
  }
}

object InterleaveOnlineExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = cg match {
      case n: NYC => (18, 40)
      case s: SSB => (14, 30)
    }
    Random.setSeed(1024)
    val logN = 9
    val dc = if (isSMS) cg.loadSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val sch = cg.schemaInstance
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new InterleaveOnlineExperiment(ename)
    //val mqr = new MaterializedQueryResult(cg, isSMS)  //for loading pre-generated queries and results
    //val query_dim = Vector(10, 12, 14, 18).reverseIterator
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