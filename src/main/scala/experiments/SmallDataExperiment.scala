package experiments

import backend.CBackend
import core.solver.SolverTools
import core.solver.moment.{CoMoment5SliceSolverDouble, CoMoment5SolverDouble, Moment1Transformer, MomentSamplingWithSlicingSolver, MomentTransformer}
import core.solver.sampling.{MomentSamplingSolver, NaiveSamplingSolver}
import core.{DataCube, PartialDataCube}
import frontend.experiments.Tools
import frontend.generators._
import util.BitUtils.{IntToSet, SetToInt}
import util.{Profiler, Util}

import java.io.PrintStream
import scala.util.Random


class SmallDataExperiment(ename2:String = "")(implicit timestampedfolder:String) extends Experiment(s"SmallDataset", ename2, "SmallDataset-expts"){
  val header = "CubeName,Query,QSize, Slicing" +
    "Solver,Fraction,Error"
  fileout.println(header)
  val solout = new PrintStream(s"expdata/solution.csv")
  def run_SmallDataExperiment(groupSize: Int, version: String)(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double]) ={
    val q = qu.sorted
    //val algo = "MomentSamplingWithSlicing"
    val algo = "NaiveSampling"
    val common =  s"$dcname,${qu.mkString(":")},${qu.size},$algo,"
    Profiler.resetAll()
    val (solver, cuboid, mask, pms, numWords, numGroups, primMoments) = Profiler(s"Prepare") {
      val pm = dc.index.prepareNaive(q).head // get prematerialized base cuboid
      val be = dc.cuboids.head.backend //get backend
      val cuboid = dc.cuboids(pm.cuboidID).asInstanceOf[be.SparseCuboid]
      assert(cuboid.n_bits == dc.index.n_bits) //number of basemoment cuboid
      val primMoments = SolverTools.preparePrimaryMomentsForQuery[Double](q, dc.primaryMoments)
      val sliceList = Vector(9 -> 1, 8 -> 1, 7 -> 1, 6 -> 1, 5 -> 1).reverse
      //val sliceList = Vector()
      val pms = dc.index.prepareBatch(q)
      //val pms_iter = dc.index.prepareBatch(q)
      val numWords = ((cuboid.size + 63) >> 6).toInt
      val numGroups = numWords >> groupSize
      val total_samples = 1024 / (1 << sliceList.size)
      //val s = new MomentSamplingWithSlicingSolver(q.size, version, sliceList, true,  Moment1Transformer[Double](), primMoments, total_samples, q)
      //val sol = new CoMoment5SliceSolverDouble(q.size, sliceList, true, Moment1Transformer[Double](), primMoments)
      val sol = new NaiveSamplingSolver(q.size-sliceList.size, 1 << (q.size-sliceList.size))
      (sol, cuboid, pm.cuboidIntersection, pms, numWords, numGroups, primMoments)
    }
    println(s"\t  Prepare Done")

    var total_cuboid_cost = 0
    val fetched_cuboids = Profiler("Fetch") {
      pms.foreach(pm => total_cuboid_cost += 1 << pm.cuboidCost)
      pms.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) }
    }
    println(s"\t Moment Fetch Done.")
    //val samples_1: Array[Long] = Array[Long](0, 15, 1, 76, 2, 46, 3, 31, 4, 4, 5, 43, 6, 14, 7, 99)
    //val samples_2: Array[Long] = Array[Long](8, 85, 9, 93, 10, 5, 11, 30, 12, 63, 13, 88, 14, 89, 15, 19)
    //val samples_3: Array[Long] = Array[Long](16, 52, 17, 9, 18, 34, 19, 39, 20, 91, 21, 88, 22, 88, 23, 4)
    //val samples_4: Array[Long] = Array[Long](24, 12, 25, 5, 26, 94, 27, 18, 28, 76, 29, 82, 30, 33, 31, 56)
    val samples_1: Array[Long] = Array[Long](0, 15, 9, 93, 2, 46, 11, 30, 4, 4, 21, 88, 6, 14, 31, 56)
    val samples_2: Array[Long] = Array[Long](8, 85, 1, 76, 18, 34, 3, 31, 13, 88, 20, 91, 22, 88, 15, 19)
    val samples_3: Array[Long] = Array[Long](16, 52, 25, 5, 26, 94, 19, 39, 28, 76, 5, 43, 30, 33, 7, 99)
    val samples_4: Array[Long] = Array[Long](24, 12, 17, 9, 10, 5, 27, 18, 12, 63, 29, 82, 23, 4, 14, 89)
    //0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    //solver.addSample(samples_1)
    //solver.addSample(samples_2)
    //solver.addSample(samples_3)
    //solver.addSample(samples_4)
    //solver.convertSampleToMoments()
    //fileout.println(solver.sampleCentralMomentsToAdd.mkString(","))
    //val moment_result = Profiler("Solve") {
      //val cols = SetToInt(List(0, 1, 2, 3, 4))
      //val data = Array[Double](1768, 1623, 1581, 2093, 1661, 1878, 1515, 2083, 1513, 1544, 1470, 1917, 1644, 1634, 1700,
      //  1455, 1492, 1600, 1577, 1482, 1639, 1761, 1690, 1720, 1310, 1896, 1771, 1402, 1810, 1752, 1282, 1898)
      //val cols = SetToInt(List(5))
      //val cl = IntToSet(cols)
      //val data = Array[Double](26129.0, 27032.0)
      //fetched_cuboids.foreach { case (cols, data) => solver.add(cols, data) }
      //solver.add(cols, data)
      //solver.convertSampleToMoments()
      //solver.fillMissing() //central to raw

      //solver.solve(true) //with heuristics to avoid negative values
    //}
    solver.addSample(samples_1)
    val naive_res_1 = solver.solve()
    fileout.println(naive_res_1.mkString(","))
    val Error_add_sample_1 = SolverTools.error(trueResult, naive_res_1)
    fileout.println(common + s"${algo},0.25,${Error_add_sample_1}")
    solver.addSample(samples_2)
    val naive_res_2 = solver.solve()
    fileout.println(naive_res_2.mkString(","))
    val Error_add_sample_2 = SolverTools.error(trueResult, naive_res_2)
    fileout.println(common + s"${algo},0.5,${Error_add_sample_2}")
    solver.addSample(samples_3)
    val naive_res_3 = solver.solve()
    fileout.println(naive_res_3.mkString(","))
    val Error_add_sample_3 = SolverTools.error(trueResult, naive_res_3)
    fileout.println(common + s"${algo},0.75,${Error_add_sample_3}")
    solver.addSample(samples_4)
    val naive_res_4 = solver.solve()
    fileout.println(naive_res_4.mkString(","))
    val Error_add_sample_4 = SolverTools.error(trueResult, naive_res_4)
    fileout.println(common + s"${algo},1.0,${Error_add_sample_4}")


    //fileout.println(primMoments.mkString(","))
    //fileout.println(trueResult.mkString(","))
    //fileout.println(solver.moments.mkString(","))
    //fileout.println(solver.sampleCentralMoments.mkString(","))
    //fileout.println(solver.ratio_All.mkString(","))
    //fileout.println(solver.timesInTotal.mkString(","))
    //fileout.println(solver.addCounter.zipWithIndex.mkString(","))
    //fileout.println(solver.moments.size.toString)
    //fileout.println(trueResult.size.toString)
    //fileout.println(moment_result.size.toString)
    //fileout.println(solver.moments_combined.mkString(","))
    //fileout.println(moment_result.mkString(","))
    //val momentError = SolverTools.error(trueResult, moment_result)
    //fileout.println(common+s"${algo},1.0,${momentError}")
    //val cuboids_input = fetched_cuboids.foreach{ case (cols, data) => (cols, data.mkString(":"))}
    //fileout.println(fetched_cuboids.size.toString)

    //val samples_1: Array[Long] = Array[Long](0, 27, 1, 99, 2, 64, 3, 29, 4, 29, 5, 20, 6, 32, 7, 85)
    //solver.addSample(samples_1)
    //solver.convertSampleToMoments()
    //fileout.println(solver.sampleCentralMomentsToAdd.mkString(","))
    //solver.fillMissing() //central to raw
    //val result_add_samples_1 = solver.solve()
    //val Error_add_sample_1 = SolverTools.error(trueResult, result_add_samples_1)
    //fileout.println(solver.moments_from_sampling.mkString(","))
    //fileout.println(solver.moments_combined.mkString(","))
    //fileout.println(common + s"${algo},0.25,${Error_add_sample_1}")
    //val samples_2: Array[Long] = Array[Long](8, 98, 9, 79, 10, 14, 11, 78, 12, 57, 13, 97, 14, 90, 15, 57)
    //val samples_3: Array[Long] = Array[Long](16, 74, 17, 91, 18, 71, 19, 91, 20, 69, 21, 56, 22, 12, 23, 81)
    //val samples_4: Array[Long] = Array[Long](24, 24, 25, 65, 26, 77, 27, 84, 28, 54, 29, 59, 30, 88, 31, 61)
    /*
    val samples_1: Array[Long] = Array[Long](992, 15, 993, 76, 994, 46, 995, 31, 996, 4, 997, 43, 998, 14, 999, 99)
    val samples_2: Array[Long] = Array[Long](1000, 85, 1001, 93, 1002, 5, 1003, 30, 1004, 63, 1005, 88, 1006, 89, 1007, 19)
    val samples_3: Array[Long] = Array[Long](1008, 52, 1009, 9, 1010, 34, 1011, 39, 1012, 91, 1013, 88, 1014, 88, 1015, 4)
    val samples_4: Array[Long] = Array[Long](1016, 12, 1017, 5, 1018, 94, 1019, 18, 1020, 76, 1021, 82, 1022, 33, 1023, 56)

    solver.addSample(samples_1)
    val result_add_samples_1 = solver.solve()
    val Error_add_sample_1 = SolverTools.error(trueResult, result_add_samples_1)
    fileout.println(common+s"${algo},0.25,${Error_add_sample_1}")

    solver.addSample(samples_2)
    val result_add_samples_2 = solver.solve()
    val Error_add_sample_2 = SolverTools.error(trueResult, result_add_samples_2)
    fileout.println(common+s"${algo},0.5,${Error_add_sample_2}")

    solver.addSample(samples_3)
    val result_add_samples_3 = solver.solve()
    val Error_add_sample_3 = SolverTools.error(trueResult, result_add_samples_3)
    fileout.println(common+s"${algo},0.75,${Error_add_sample_3}")

    solver.addSample(samples_4)
    val result_add_samples_4 = solver.solve()
    val Error_add_sample_4 = SolverTools.error(trueResult, result_add_samples_4)
    fileout.println(common+s"${algo},1.0,${Error_add_sample_4}")

     */

  }


  override def run(dc: DataCube, dcname: String, qu: IndexedSeq[Int], trueResult: Array[Double], output: Boolean = true, qname: String = "", sliceValues: Seq[(Int, Int)] = Nil): Unit = {
    val increment_pm = 1
    val increment_sample = 6400
    // run_ZipfExperiment(0, "V1", increment_pm, increment_sample, 0.0, 1L)(dc, dcname, qu, trueResult)
    run_SmallDataExperiment(1, "V1")(dc, dcname, qu, trueResult)
  }

}


object SmallDataExperiment extends ExperimentRunner {
  def qsize(cg: CubeGenerator, isSMS: Boolean)(implicit timestampedFolder: String, numIters: Int, be: CBackend) = {
    val (minD, maxD) = (4, 9)
    Random.setSeed(1024)
    val logN = 5
    val dc = if (isSMS) cg.loadPreForTenSMS(logN, minD, maxD) else cg.loadRMS(logN, minD, maxD)
    dc.loadPrimaryMoments(cg.baseName)
    val sch = cg.schemaInstance
    val ename = s"${cg.inputname}-$isSMS-qsize"
    val expt = new SmallDataExperiment(ename)
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
          val slice = Vector(9 -> 1, 8 -> 1, 7 -> 1, 6 -> 1, 5 -> 1).reverse
          //val slice = Vector()
          val trueResult = Util.slice(dc.naive_eval(query), slice)
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
    val smd = new SmallDatasetGenerator()

    def func(param: String)(timestamp: String, numIters: Int) = {
      implicit val ni = numIters
      implicit val ts = timestamp
      param match {
        case "smalldataset-prefix" => qsize(smd, true)
      }
    }

    run_expt(func)(args)
  }
}
