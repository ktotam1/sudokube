package examples

import backend._
import breeze.linalg.DenseMatrix
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.github.tototoshi.csv.CSVReader
import core._
import core.materialization._
import core.solver.RationalTools._
import core.solver.SolverTools.error
import core.solver._
import core.solver.iterativeProportionalFittingSolver.{EffectiveIPFSolver, IPFUtils, VanillaIPFSolver}
import core.solver.moment._
import frontend._
import frontend.experiments.Tools
import frontend.generators._
import frontend.gui.{FeatureFrameSSB, MaterializationView, QueryView}
import frontend.schema.DynamicSchema
import frontend.schema.encoders.{ColEncoder, DynamicColEncoder, MemCol}
import util.BitUtils._
import util._

import java.io.FileWriter
import scala.reflect.ClassTag
import scala.util.Random

object DemoTxt {

  /** Example for Moment Solver */
  def momentSolver[T:ClassTag:Fractional]() = {
    val num = implicitly[Fractional[T]]
    //0 and 1D moments are required for MomentSolver that we precompute here
    val pm = List(0 -> 17, 1 -> 4, 2 -> 7, 4 -> 12).map(x => x._1 -> num.fromInt(x._2))
    val total = 3 //total query bits
    val slice = Vector(0->0).reverse //slicing for the top k-bits in the order of least significant to most significant
    val agg = total - slice.length //how many bits for aggregation

    val solver = new CoMoment5SliceSolver[T](total, slice, true, Moment1Transformer(), pm)
    //val solver = new CoMoment5SliceSolver[T](total ,slice,true, Moment1Transformer(), pm)
    //val solver = new CoMoment5Solver[T](total ,true, Moment1Transformer(), pm)

    //true result
    val actual = Util.slice(Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble), slice)
    implicit def listToInt = SetToInt(_)

    //Add 2D cuboid containing bits 0,1 to solver
    //solver.add(List(0, 1), Array(7, 3, 6, 1).map(x => num.fromInt(x)))

    //Add 2D cuboid containing bits 1,2 to solver
    solver.add(List(1, 2), Array(1, 4, 9, 3).map(x => num.fromInt(x)))

    //Add 2D cuboid containing bits 0,2 to solver
    //solver.add(List(0, 2), Array(3, 2, 10, 2).map(x => num.fromInt(x)))

    println("Moments before =" + solver.moments.mkString(" "))
    //extrapolate missing moments
    solver.fillMissing()
    println("Moments after =" + solver.moments.mkString(" "))

    //convert extrapolated moments to values
    val result = solver.solve()
    println(result.map(num.toDouble(_)).mkString(" "))
    println("Error = " + error(actual, result))
  }

  def momentSamplingSolverWithSlicing() = {

    //0 and 1D moments are required for MomentSolver that we precompute here
    val pm = List(0 -> 53161.0, 1 -> 27738.0, 2 -> 26636.0, 4 -> 27122.0, 8 -> 25998.0, 16 -> 26082.0, 32 -> 26404.0,
      64 -> 27032.0, 128 -> 26255.0, 256 -> 26422.0, 512 -> 27116.0)
    val total = 3 //total query bits
    val slice = Vector(5 -> 0, 6 -> 0, 7 -> 0, 8 -> 0, 9 -> 0).reverse //slicing for the top k-bits in the order of least significant to most significant
    val agg = total - slice.length //how many bits for aggregation
    val q = Vector(0,1,2,3,4).toIndexedSeq
    val total_samples = 32
    val solver = new MomentSamplingWithSlicingSolver(total, q.size,"V3", slice, true, Moment1Transformer[Double](), pm, total_samples, q)
    //val solver = new CoMoment5SliceSolver[T](total ,slice,true, Moment1Transformer(), pm)
    //val solver = new CoMoment5Solver[T](total ,true, Moment1Transformer(), pm)
    //val samples: Array[Long] = Array[Long](0, 0, 1, 1, 2, 3, 3, 1, 4, 7, 5, 2, 6, 3, 7, 0)
    //true result
    //val samples: Array[Long] = Array[Long](1, 1, 3, 1, 5, 2, 7, 0)
    //val samples: Array[Long] = Array[Long](0, 0, 2, 3, 4, 7, 6, 3)
    //val samples: Array[Long] = Array[Long](4, 7, 6, 3)
    //solver.addSample(samples)
    //val actual = Util.slice(Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble), slice)
    val actual = Util.slice(Array(18, 50, 35, 58, 24, 79, 61, 78, 12, 64, 56, 70, 97, 3, 16, 45, 45, 48, 70, 33, 61, 94,
      6, 5, 10, 50, 50, 43, 81, 96, 90, 97, 3, 69, 24, 57, 15, 35, 100, 79, 4, 11, 12, 66, 36, 30, 45, 71, 20, 92, 34,
      77, 85, 71, 77, 55, 4, 96, 22, 17, 84, 92, 45, 44, 87, 77, 27, 65, 19, 28, 65, 86, 23, 66, 14, 64, 60, 32, 17, 73,
      84, 4, 87, 8, 2, 1, 86, 25, 40, 68, 22, 25, 31, 100, 29, 89, 36, 15, 65, 59, 88, 42, 49, 88, 7, 19, 18, 66, 81, 71,
      63, 28, 73, 42, 63, 50, 72, 5, 33, 81, 19, 90, 9, 33, 39, 12, 48, 23, 96, 22, 78, 83, 76, 63, 90, 79, 96, 93, 63,
      89, 19, 64, 50, 35, 43, 2, 53, 5, 75, 98, 8, 28, 13, 81, 20, 39, 21, 88, 43, 51, 11, 92, 25, 79, 48, 84, 41, 50,
      51, 55, 60, 19, 75, 60, 95, 52, 69, 71, 38, 21, 31, 56, 81, 54, 25, 9, 36, 32, 73, 59, 19, 28, 32, 18, 63, 51, 33,
      38, 72, 41, 21, 52, 14, 94, 64, 52, 98, 33, 86, 38, 49, 84, 45, 60, 19, 54, 18, 66, 68, 67, 56, 60, 32, 62, 96, 11,
      87, 99, 9, 92, 57, 56, 12, 94, 72, 68, 55, 4, 51, 62, 48, 39, 5, 69, 92, 32, 76, 39, 88, 29, 64, 59, 52, 53, 95,
      66, 12, 18, 49, 92, 54, 68, 63, 32, 86, 99, 18, 50, 21, 46, 61, 34, 65, 27, 100, 13, 97, 55, 2, 79, 50, 90, 79, 8,
      79, 79, 84, 6, 55, 29, 30, 71, 96, 82, 1, 77, 21, 35, 100, 96, 39, 12, 9, 14, 32, 50, 64, 92, 98, 60, 79, 74, 6,
      17, 42, 67, 31, 8, 43, 97, 57, 97, 100, 66, 15, 72, 59, 93, 20, 22, 36, 96, 78, 11, 47, 48, 38, 62, 78, 61, 85,
      87, 74, 81, 59, 83, 82, 84, 97, 97, 15, 99, 62, 88, 20, 53, 85, 1, 67, 83, 11, 71, 7, 15, 87, 43, 51, 6, 8, 31, 43,
      8, 8, 70, 2, 20, 19, 98, 91, 54, 32, 4, 52, 81, 67, 15, 27, 45, 74, 47, 19, 39, 25, 91, 53, 81, 5, 45, 53, 53, 41,
      98, 1, 11, 53, 96, 53, 93, 73, 86, 54, 8, 89, 77, 51, 65, 76, 3, 90, 15, 36, 54, 85, 65, 51, 42, 63, 76, 28, 25,
      65, 28, 83, 28, 41, 49, 15, 72, 6, 46, 59, 28, 15, 45, 77, 40, 9, 73, 53, 81, 89, 96, 93, 49, 47, 30, 74, 1, 8, 29,
      84, 39, 46, 13, 13, 52, 89, 58, 40, 88, 23, 1, 21, 5, 99, 6, 71, 55, 31, 68, 100, 59, 25, 22, 58, 77, 13, 49, 88,
      41, 75, 19, 22, 43, 63, 10, 18, 79, 2, 69, 53, 80, 47, 65, 36, 12, 13, 23, 53, 60, 68, 20, 82, 90, 12, 98, 61, 100,
      24, 82, 87, 90, 99, 52, 66, 87, 28, 47, 72, 36, 67, 1, 14, 77, 77, 61, 43, 4, 67, 23, 38, 90, 15, 77, 56, 8, 14, 63,
      86, 96, 7, 53, 90, 24, 31, 74, 35, 88, 92, 92, 81, 81, 86, 94, 39, 34, 47, 27, 82, 92, 84, 4, 90, 91, 36, 94, 56,
      76, 37, 99, 30, 63, 33, 72, 67, 6, 37, 59, 67, 34, 85, 83, 47, 51, 1, 5, 78, 60, 84, 63, 31, 72, 91, 54, 67, 40,
      51, 52, 32, 47, 62, 26, 78, 57, 23, 85, 30, 26, 2, 90, 82, 15, 47, 93, 19, 69, 10, 69, 41, 11, 90, 98, 32, 61, 23,
      95, 29, 43, 74, 62, 66, 17, 61, 100, 93, 42, 80, 70, 8, 13, 20, 33, 15, 30, 38, 18, 74, 39, 8, 99, 55, 71, 20, 72,
      16, 83, 75, 24, 6, 99, 8, 28, 56, 53, 56, 52, 35, 42, 8, 95, 55, 56, 76, 29, 47, 61, 72, 68, 14, 75, 70, 53, 8, 18,
      10, 98, 77, 1, 97, 5, 51, 25, 27, 95, 98, 40, 55, 63, 23, 50, 86, 21, 93, 57, 10, 60, 100, 92, 50, 3, 23, 55, 20,
      89, 46, 33, 80, 5, 32, 10, 94, 53, 32, 40, 4, 64, 25, 96, 57, 19, 57, 29, 28, 100, 49, 8, 61, 57, 29, 82, 85, 90,
      51, 18, 87, 60, 99, 19, 67, 32, 77, 100, 70, 85, 96, 74, 15, 18, 17, 94, 44, 24, 18, 53, 30, 73, 15, 42, 74, 34,
      69, 77, 35, 20, 19, 88, 62, 23, 27, 49, 73, 83, 1, 64, 62, 91, 33, 12, 22, 70, 22, 44, 53, 71, 16, 79, 37, 22, 94,
      64, 33, 49, 97, 93, 51, 24, 82, 4, 73, 41, 12, 19, 17, 94, 49, 88, 74, 47, 31, 5, 38, 10, 64, 73, 51, 90, 96, 81,
      23, 70, 72, 21, 84, 80, 30, 54, 4, 78, 82, 4, 17, 58, 26, 41, 62, 68, 4, 89, 88, 37, 42, 19, 7, 17, 56, 43, 3, 97,
      34, 25, 72, 94, 85, 62, 53, 77, 34, 63, 25, 88, 55, 89, 87, 95, 65, 64, 94, 17, 30, 89, 51, 60, 76, 98, 38, 93, 99,
      37, 69, 80, 22, 55, 4, 18, 89, 11, 62, 68, 3, 94, 20, 10, 6, 22, 23, 19, 77, 49, 70, 54, 82, 69, 30, 62, 65, 10, 57,
      62, 62, 29, 78, 64, 13, 5, 26, 59, 37, 79, 64, 7, 4, 51, 74, 93, 19, 74, 40, 12, 41, 18, 68, 55, 68, 51, 74, 49,
      98, 57, 51, 77, 40, 55, 74, 88, 89, 90, 58, 72, 33, 30, 53, 71, 28, 41, 97, 42, 41, 69, 85, 52, 50, 76, 85, 87, 48,
      37, 24, 56, 47, 15, 10, 98, 76, 77, 21, 48, 87, 100, 87, 72, 7, 99, 42, 79, 87, 92, 67, 1, 41, 63, 24, 4, 49, 97,
      74, 6, 98, 78, 1, 47, 43, 54, 75, 12).map(_.toDouble), slice)

    implicit def listToInt = SetToInt(_)

    //Add 2D cuboid containing bits 0,1 to solver
    //solver.add(List(0, 1), Array(7.0, 3.0, 6.0, 1.0))

    //Add 2D cuboid containing bits 1,2 to solver
    //solver.add(List(1, 2), Array(1.0, 4.0, 9.0, 3.0))

    //Add 2D cuboid containing bits 0,2 to solver
    //solver.add(List(0, 2), Array(3.0, 2.0, 10.0, 2.0))
    //solver.add(List(2), Array(5.0, 12.0))
    //solver.add(List(1), Array(10.0, 7.0))
    //solver.add(List(0), Array(13, 4))
    println("Moments before =" + solver.moments.mkString(" "))
    //extrapolate missing moments
    solver.fillMissing()
    println("Moments after =" + solver.moments.mkString(" "))

    //convert extrapolated moments to values
    val result = solver.solve()
    println(actual.mkString(" "))
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
  }


  def vanillaIPFSolver(): Unit = { // Bad case for IPF — 2000+ iterations
    val actual = Array(1, 1000, 1000, 1000, 1000, 1000, 1000, 1).map(_.toDouble)
    val solver = new VanillaIPFSolver(3, true, false, actual)
    solver.add(BitUtils.SetToInt(List(0, 1)), Array(1001, 2000, 2000, 1001).map(_.toDouble))
    solver.add(BitUtils.SetToInt(List(1, 2)), Array(1001, 2000, 2000, 1001).map(_.toDouble))
    solver.add(BitUtils.SetToInt(List(0, 2)), Array(1001, 2000, 2000, 1001).map(_.toDouble))
    val result = solver.solve()
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  def effectiveIPFSolver(): Unit = { // Decomposable
    val randomGenerator = new Random()
    val actual: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => actual(i) = randomGenerator.nextInt(100))

    val solver = new EffectiveIPFSolver(6)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(0, 3, 4), Seq(4, 5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, actual, marginalVariables.size, SetToInt(marginalVariables))
      ).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(BitUtils.SetToInt(marginalVariables), clustersDistribution)
    }

    val result = solver.solve()
    println(actual.mkString(" "))
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  def vanillaIPFSolver2(): Unit = { // Decomposable, just for comparison
    val randomGenerator = new Random()
    val actual: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => actual(i) = randomGenerator.nextInt(100))

    val solver = new VanillaIPFSolver(6, true, false, actual)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(0, 3, 4), Seq(4, 5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, actual, marginalVariables.size, SetToInt(marginalVariables))
      ).toMap

    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(BitUtils.SetToInt(marginalVariables), clustersDistribution)
    }

    val result = solver.solve()
    println(actual.mkString(" "))
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  def momentSolver3(): Unit = { // Decomposable, just for comparison
    val randomGenerator = new Random()
    val actual: Array[Double] = Array.fill(1 << 6)(0)
    (0 until 1 << 6).foreach(i => actual(i) = randomGenerator.nextInt(100))

    val solver = new MomentSolverAll[Rational](6)
    val marginalDistributions: Map[Seq[Int], Array[Double]] =
      Seq(Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(0, 3, 4), Seq(4, 5)).map(marginalVariables =>
        marginalVariables -> IPFUtils.getMarginalDistribution(6, actual, marginalVariables.size, SetToInt(marginalVariables))
      ).toMap
    implicit def listToInt = SetToInt(_)
    marginalDistributions.foreach { case (marginalVariables, clustersDistribution) =>
      solver.add(marginalVariables, clustersDistribution.map(n => Rational(BigInt(n.toInt), 1)))
    }
    solver.fillMissing()

    val result = solver.fastSolve().map(_.toDouble)
    println(actual.mkString(" "))
    println(result.mkString(" "))
    println("Error = " + error(actual, result))
    solver.verifySolution()
  }

  /** Demo for creating new data cube without using UserCube frontend */
  def sales_demo(): Unit = {
    //Set backend
    implicit val be = CBackend.default

    /**
    Load cube generator for sales data in file `tabledata/TinyData/data.csv``
     */
    //val cubeGenerator = new TinyDataStatic()
    val cubeGenerator = new ZipfGenerator()
    val schema = cubeGenerator.schemaInstance

    val cubename = "myzipfcube"
    println("cubeGenerator has been prepared")
    /* ----------------- Building Data cube. TO BE RUN ONLY ONCE  -------------------*/
    /** Load base cuboid of the dataset and generate it if it does not exist
     * See also [[frontend.generators.CubeGenerator#saveBase()]] */
    val baseCuboid = cubeGenerator.loadBase(true)



    /** Define instance of [[core.materialization.MaterializationStrategy]]
     *  See also [[RandomizedMaterializationStrategy]], [[SchemaBasedMaterializationStrategy]]
     * */

    println("Start preparing the mstrat")
    val mstrat = PresetMaterializationStrategy(schema.n_bits, Vector(
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
      Vector(9, 12, 13, 17),
      Vector(4, 6, 7, 11),
      Vector(6, 12, 15, 16),
      Vector(2, 5, 9, 14),
      (0 until schema.n_bits) //base cuboid must be always included at last position
    ))
    println("mstats has been prepared")

    /*
        val mstrat = new PresetMaterializationStrategy(schema.n_bits, Vector(
          Vector(0, 1),
          Vector(1, 3),
          Vector(0, 2, 3),
          (0 until schema.n_bits) //base cuboid must be always included at last position
        ))

     */

    /** Build data cube. See also [[frontend.generators.CubeGenerator#saveSMS(int, int, int)]] */
    /** We store as partial data cube with external reference to base cuboid to avoid storing
     * base cuboid twice */
    val dataCube = new PartialDataCube(cubename, cubeGenerator.baseName)
    println("dataCube has been prepared")
    dataCube.buildPartial(mstrat)
    dataCube.save()
    println("saved the dataCube")
    //Compute 1-D marginals and total
    dataCube.primaryMoments = SolverTools.primaryMoments(dataCube)
    dataCube.savePrimaryMoments(cubeGenerator.baseName)
    val dc = PartialDataCube.load(cubename, cubeGenerator.baseName)
    dc.loadPrimaryMoments(cubeGenerator.baseName)
/*
    /*------------------------- Loading Data Cube ---------------------*/
    val dc = PartialDataCube.load(cubename, cubeGenerator.baseName)
    dc.loadPrimaryMoments(cubeGenerator.baseName)

    //val query = Vector(0, 1, 3)
    /** See also [[frontend.service.MyDimLevel]] */
    val colMap = schema.columnVector.map { e => e.name -> e.encoder.bits }.toMap


    val query = (colMap("Quarter").takeRight(1) ++ colMap("City")).sorted


    /** Measure Execution Time using [[Profiler]]. First clear everything */
    Profiler.resetAll()

    val trueResult = Profiler("NaiveTime") {
      dc.naive_eval(query) // Querying using Naive Solver
    }

    /*------------------- Querying using Moment Solver-----------*/

    /**
     * Prepare phase.
     * Find cuboids relevant to answering query.
     * Find primary moments associated with dimensions in query
     */

    val (prepared, pm) = Profiler("PrepareMoment") {
      dc.index.prepareBatch(query) -> SolverTools.preparePrimaryMomentsForQuery[Double](query, dc.primaryMoments)
    }

    /** Fetch phase
     * Fetch cuboids from backend after projecting down to relevant dimensions
     * */
    val fetched = Profiler("FetchMoment") {
      prepared.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) }
    }

    /** Solve phase using moment solver */
    val result = Profiler("SolveMoment") {
      val s = new CoMoment5SolverDouble(query.size, true, null, pm)
      fetched.foreach { case (cols, data) => s.add(cols, data) }
      s.fillMissing()
      s.solve(true) //with heuristics to avoid negative values
    }
    val prepareTime = Profiler.getDurationMicro("PrepareMoment")
    val fetchTime = Profiler.getDurationMicro("FetchMoment")
    val solveTime = Profiler.getDurationMicro("SolveMoment")
    val totalTime = prepareTime + fetchTime + solveTime
    val naiveTime = Profiler.getDurationMicro("NaiveTime")
    val error = SolverTools.error(trueResult, result)
    fetched.foreach{x => println(x._1 + "  ::  " + x._2.mkString(" "))}
    println(s"""
            Query: ${query.mkString(",")}
            True Result: ${trueResult.mkString(" ")}
            Approx Result: ${result.mkString("  ")}

            PrepareTime: $prepareTime us
            FetchTime: $fetchTime us
            SolveTime: $solveTime us
            TotalTime: $totalTime us
            Error: $error

            NaiveTime: $naiveTime us
            """)

 */
  }


  /** Demo for frontend stuff */
  def cooking_demo(): Unit = {

    val cube = UserCube.createFromJson("example-data/demo_recipes.json", "rating", "demoCube")

    //save/load matrices
    cube.save()
    val userCube = UserCube.load("demoCube")

    //can query for a matrix
    var matrix = userCube.query(Vector(("price", 1, Nil)), Vector(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")

    // can add several dimensions, internal sorting
    matrix = userCube.query(Vector(("Region", 2, Nil), ("price", 1, Nil)), Vector(("time", 3, Nil)), AND, MOMENT, MATRIX).asInstanceOf[DenseMatrix[String]]
    println(matrix.toString(Int.MaxValue, Int.MaxValue) + "\n \n")

    //can query for an array, return top/left/values
    val tuple = userCube.query(Vector(("Region", 2, Nil)), Vector(("time", 2, Nil)), AND, MOMENT, ARRAY).asInstanceOf[(Array[Any],
      Array[Any], Array[Any])]
    println(tuple._1.mkString("top header\n(", ", ", ")\n"))
    println(tuple._2.mkString("left header\n(", "\n ", ")\n"))
    println(tuple._3.mkString("values\n(", ", ", ")\n \n"))

    //can query for array of tuples with bit format
    var array = userCube.query(Vector(("Type", 2, Nil), ("price", 2, Nil)), Vector(), AND, MOMENT, TUPLES_BIT).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can query for array of tuples with prefix format
    array = userCube.query(Vector(("Type", 2, Nil), ("price", 2, Nil)), Vector(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can slice and dice: select (Type = Dish || Type = Side) && price = cheap
    array = userCube.query(Vector(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Vector(), AND, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //select (Type = Dish || Type = Side) || price = cheap
    array = userCube.query(Vector(("Type", 2, List("Dish", "Side")), ("price", 2, List("cheap"))), Vector(), OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))
    //delete zero tuples
    array = ArrayFunctions.deleteZeroColumns(array)
    println(array.mkString("(", "\n ", ")\n \n"))

    array = userCube.query(Vector(("Type", 0, Nil)), Vector(), OR, MOMENT, TUPLES_PREFIX).asInstanceOf[Array[
      Any]].map(x => x.asInstanceOf[(String, Any)])
    println(array.mkString("(", "\n ", ")\n \n"))

    //can apply some binary function
    println(ArrayFunctions.applyBinary(array, binaryFunction, ("price", "Type"), EXIST))
    println(ArrayFunctions.applyBinary(array, binaryFunction, ("price", "Type"), FORALL) + "\n \n")

    def binaryFunction(str1: Any, str2: Any): Boolean = {
      str1.toString.equals("cheap") && !str2.toString.equals("Dish")
    }

    def transformForGroupBy(src: String): String = {
      src match {
        case "Europe" | "Italy" | "France" => "European"
        case _ => "Non-European"
      }
    }

    //can query another dimension, with TUPLES_PREFIX format
    println(userCube.queryDimension(("time", 4, Nil), "difficulty", MOMENT))
    println(userCube.queryDimension(("difficulty", 4, Nil), null, MOMENT) + "\n \n")

    //can detect double peaks and monotonicity
    println(userCube.queryDimensionDoublePeak(("time", 4, Nil), "difficulty", MOMENT, 0.0))
    println(userCube.queryDimensionDoublePeak(("difficulty", 4, Nil), null, MOMENT, 0.0))

  }


  /** Demo for running different queries using Naive solver to understand how the backend handles projections */
  def backend_naive() = {
    val n_bits = 10
    val n_rows = 40
    val n_queries = 10
    val query_size = 5
    val rnd = new Random(1L)
    val data = (0 until n_rows).map(i => BigBinary(rnd.nextInt(1 << n_bits)) -> rnd.nextInt(10).toLong)
    implicit val backend = CBackend.default
    val fullcub = backend.mk(n_bits, data.toIterator)
    println("Full Cuboid data = " + data.mkString("  "))
    val dc = new DataCube()
    val m = new RandomizedMaterializationStrategy(n_bits, 6, 2)
    dc.build(fullcub, m)
    (0 until n_queries).map { i =>
      val q = Tools.rand_q(n_bits, query_size)
      println("\nQuery =" + q)
      val res = dc.naive_eval(q)
      println("Result = " + res.mkString(" "))
    }
  }

  /** GUI demo for SSB */
  def ssb_demo() = {
    implicit val backend = CBackend.default
    val sf = 100
    val cg = SSB(sf)
    val logN = 15
    val minD = 14
    val maxD = 30

    /** WARNING: This cube must have been built already. See [[frontend.generators.SSBGen]] * */
    val dc = cg.loadSMS(logN, minD, maxD)
    val display = FeatureFrameSSB(sf, dc, 50)
  }

  def demo(): Unit = {
    import frontend.schema._
    implicit val be = backend.CBackend.colstore
    val cg = new WebshopSales()
    cg.saveBase()
    val dc = cg.loadBase()
    val sch = cg.schemaInstance
     println("NumRows = " + dc.cuboids.last.size)
    println("NumBits = " + sch.n_bits + "   " + dc.index.n_bits)
    sch.columnVector.foreach{case LD2(n, e) => println(n + " -> " + e.bits.mkString(","))}
    println(dc.naive_eval(Vector(0, 26, 27)).mkString(" "))
    val display = new QueryView(sch, dc)
    val d2 = new MaterializationView()
  }

  def genDynSchemaData(): Unit = {
    case class SchemaChange(id: Int, newName: String, rownum: Int)
    val changes = collection.mutable.PriorityQueue[SchemaChange]()(Ordering.by(x => -x.rownum))

    changes += SchemaChange(6, "CompanyID", 20)
    //changes += SchemaChange(6, "CompanyNumber", 40)
    changes += SchemaChange(7, "CompanyName", 31)
    //changes += SchemaChange(13, "CountryName", 43)
    //changes += SchemaChange(8, "Category", 84)
    //changes += SchemaChange(9, "Product", 85)
    //changes += SchemaChange(4, "WeekNo", 100)
    //changes += SchemaChange(1, "SaleYr", 130)

    val filename = s"tabledata/Webshop/sales.csv"
    val datasize = CSVReader.open(filename).iterator.drop(1).size
    val allData = CSVReader.open(filename).all().toVector

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val outfilename = s"tabledata/Webshop/salesDyn.json"
    val fileWriter = new FileWriter(outfilename)
    val sequenceWriter = mapper.writerWithDefaultPrettyPrinter().writeValuesAsArray(fileWriter)

    val currentHeader = allData.head.toArray
    currentHeader.zipWithIndex.foreach{case (h, i) => println(i, h)}
    var nextChange = changes.head
    allData.tail.zipWithIndex.foreach { case (d, i) =>
      if (!changes.isEmpty) {
        while (i == nextChange.rownum && !changes.isEmpty) {
          currentHeader(nextChange.id) = nextChange.newName
          //println("Changing header " + nextChange.id + "  to  " + nextChange.newName + " at row " + i)
            changes.dequeue()
          if (!changes.isEmpty) {
            nextChange = changes.head
          }
        }
      }
      val row = d.toVector.zip(currentHeader).map { case (v, k) => (k, v) }.toMap
      sequenceWriter.write( row )
    }
    sequenceWriter.close()
  }

  def demo2() = {
    implicit val be  = CBackend.rowstore
    val sch = new DynamicSchema
    val data = sch.read("multi7.json" )
    val m = new DynamicSchemaMaterializationStrategy(sch, 10, 5, 15)
    val basename = "multischema"
    val dc = new DataCube(basename)
    val baseCuboid = be.mkAll(sch.n_bits, data)
    dc.build(baseCuboid, m)
    dc.save()
    dc.primaryMoments = SolverTools.primaryMoments(dc)
    dc.savePrimaryMoments(basename)
    println("nbits = " + sch.n_bits)
    println("nrows = " + data.size)
    dc.cuboids.groupBy(_.n_bits).mapValues(_.map(_.numBytes).sum).toList.sortBy(_._1).foreach{println}

    implicit def toDynEncoder[T](c: ColEncoder[T]) = c.asInstanceOf[DynamicColEncoder[T]]
    val alltimebits = sch.columns("ID").bits
    val numallTimeBits = alltimebits.size
    val n = "col1.col8"
    val en = sch.columns(n)
    println("Time bits = " + sch.columns("ID").bits + s"[${sch.columns("ID").isNotNullBit}]")
    //sch.columnList.foreach{ case (n, en) =>
      println("Col " + n + " :: " + en.bits + s"[${en.isNotNullBit}] =  " + (en.bits.size+1) + " bits")
     var continue = true
      var numsliceTimeBits = 0
      val slice  = collection.mutable.ArrayBuffer[(Int, Int)]()
      slice += en.isNotNullBit -> 1
      while(continue) {
        val end =  numallTimeBits-numsliceTimeBits
        val q =  ((end - 1) until end).map(alltimebits(_))
        val slicedims = slice.map(_._1)
        println("Current sliceDimensions = " + slicedims.mkString(" "))
        println("Current sliceValue = " + slice.map(_._2).mkString(" "))
        println("Current query = " + q)
        val qsorted = (q ++ slicedims).sorted
        println("Full query = " + qsorted)
        val qres = dc.naive_eval(qsorted)
        val slice2 = slice.map{case (b, v) => qsorted.indexOf(b) -> v}.sortBy(_._1)

        val list = dc.index.prepareBatch(qsorted)
        list.foreach{pm =>
          val present = BitUtils.IntToSet(pm.queryIntersection).map(i => qsorted(i))
          val missing = qsorted.diff(present)
          println(s"Present = ${present.mkString(" ")}  Missing = ${missing.mkString(" ")}  Cost = ${pm.cuboidCost} #Present=${pm.queryIntersectionSize}")
        }

        val qresslice = Util.slice(qres, slice2)
        //println("True result = " + qres.mkString("  "))
        println("True Slice result = " + qresslice.mkString("  "))


        val fetched = list.map{ pm =>(pm.queryIntersection, dc.fetch2[Double](List(pm)))}

        val ipfsolver = new VanillaIPFSolver(qsorted.size)
        fetched.foreach { case (bits, array) => ipfsolver.add(bits, array) }
        val ipfres = ipfsolver.solve()
        val ipfslice = Util.slice(ipfres, slice2)
        //println("IPF result = " + ipfres.mkString("  "))
        println("IPF Slice result = " + ipfslice.mkString("  "))
        val ipferror = SolverTools.error(qres, ipfres)
        //println("IPF error =" + ipferror)

        val primaryMoments = SolverTools.preparePrimaryMomentsForQuery[Double](qsorted, dc.primaryMoments)
        val momentsolver = new CoMoment5SliceSolver[Double](qsorted.size, slice2, true, new Moment1Transformer, primaryMoments)
        fetched.foreach{ case (bits, array) => momentsolver.add(bits, array) }
        momentsolver.fillMissing()
        val momentres = momentsolver.solve()
        println("Moment Slice result = " + momentres.mkString("  "))

        println("Enter slice bit: ")
        val sbit: Int = scala.io.StdIn.readLine().toInt
        continue = sbit < 2
        slice += q.max -> sbit
        numsliceTimeBits += 1
      }
    //}
  }

  def online_demo(): Unit = {
    implicit val be = CBackend.default
    val cg = new TinyDataStatic()
    val sch = cg.schemaInstance

    val dc = cg.loadBase()
    val baseCuboid = dc.cuboids.last.asInstanceOf[be.SparseCuboid]

    val data1 = baseCuboid.fetch64(0)
    println("Initial")
    data1.filter(x => x._1.toBigInt != 0 || x._2 !=0).foreach(println)

    baseCuboid.randomShuffle()

    val data2 = baseCuboid.fetch64(0)
    println("After Shuffle")
    data2.filter(x => x._1.toBigInt != 0 || x._2 !=0).foreach(println)

  }
  def main(args: Array[String]): Unit = {
    //momentSolver()
    momentSamplingSolverWithSlicing()
    //backend_naive()
    //ssb_demo()
    //cooking_demo()
    //vanillaIPFSolver()
    //effectiveIPFSolver()
    //vanillaIPFSolver2()
    //momentSolver3()
    //demo()
    //genDynSchemaData()
    //sales_demo()
    //demo2()
  }
}