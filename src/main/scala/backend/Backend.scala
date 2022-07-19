//package ch.epfl.data.sudokube
package backend
import combinatorics.Big
import util.BigBinary


/** this is a proxy. */
abstract class Cuboid {

  /**
   * Number of dimensions in this cuboid
   */
  val n_bits : Int

  /**
   * @return Number of non-zero cells in case of Sparse Cuboid and number of cells in case of Dense Cuboid
   */
  def size: BigInt

  /**
   * @return Storage space for this cuboid, in terms of bytes.
   */
  def numBytes: Long

  /**
   * Garbage collect this cuboid and de-allocate space (only for CBackend)
   */
  def gc: Unit

  /**
   * Type to specify what dimensions we want to keep after projection. For example, if a cuboid contains dimensions
   * {1, 4, 6, 7}  and we want to project to dimensions {4, 6}, we specify {1, 2} as the bit positions
   */
  type BITPOS_T = IndexedSeq[Int]
  /** Rehash this cuboid to Sparse Cuboid after projection
   * @param bitpos Indexes of dimensions we want to keep after projection
   * @return Projected Cuboid
   */
  def rehash_to_sparse(bitpos: BITPOS_T): Cuboid
  /** Rehash this cuboid to Dense Cuboid after projection
   * @param bitpos Indexes of dimensions we want to keep after projection
   * @return Projected Cuboid
   */
  def rehash_to_dense( bitopos : BITPOS_T): Cuboid
  /** Rehash this cuboid to either Sparse or Dense Cuboid smartly after projection
   * @param bitpos Indexes of dimensions we want to keep after projection
   * @return Projected Cuboid
   */
  def rehash(bitpos: BITPOS_T): Cuboid
  def backend: Backend[_]
}

/**
 * Class that encapsulates cuboid projection and loading
 * @tparam MEASURES_T  Type for the fact values in each cell of the cuboid, currently [[Payload]] that stores sum and interval
 *                     TODO: Change to Long.
 */
abstract class Backend[MEASURES_T] {
  /**
   * Identifier for Sparse Cuboid
   */
  protected type SPARSE_T
  /**
   * Identifier for Dense Cuboid
   */
  protected type DENSE_T
  /**
   * Type that encapsulates an identifier for a Cuboid along with whether it is Sparse or Dense
   */
  protected type HYBRID_T
  /**
   * Type to specify what dimensions we want to keep after projection. For example, if a cuboid contains dimensions
   * {1, 4, 6, 7}  and we want to project to dimensions {4, 6}, we specify {1, 2} as the bit positions
   */
  type BITPOS_T = IndexedSeq[Int]

  /**
   * Used in CBackend to unload all cuboids from RAM
   */
  def reset: Unit
  /**
   * @param h Encoding for Dense/Sparse cuboids
   * @return true if `h` represents a Dense cuboid
   */
  def isDense(h: HYBRID_T): Boolean
  /**
   * Converts identifier for Dense/Sparse cuboids to that for Dense Cuboids. Must be called only if the cuboid is actually
   * [[DenseCuboid]]
   */
  def extractDense(h: HYBRID_T): DENSE_T
  /**
   * Converts identifier for Dense/Sparse cuboids to that for Sparse Cuboids. Must be called only if the cuboid is actually
   * [[SparseCuboid]]
   */
  def extractSparse(h: HYBRID_T): SPARSE_T
  /**
   * Encodes a given [[SPARSE_T]] identifier into [[HYBRID_T]]
   */
  def sparseToHybrid(s: SPARSE_T): HYBRID_T

  /**
   * Encodes a given [[DENSE_T]] identifier into [[HYBRID_T]]
   */
  def denseToHybrid(d: DENSE_T): HYBRID_T


  protected val be_this = this

  /**
   * Reads a file storing the data from multiple cuboids and loads them into memory
   * @param filename Name of the file
   * @param idArray Array storing, for each cuboid stored in the file, the index of the projection (stored by this Cuboid) among all projections materialized for a given Data Cube
   * @param isSparseArray Array storing, for each cuboid stored in the file, whether the cuboid is Sparse or not
   * @param nbitsArray Array storing, for each cuboid stored in the file, the number of dimensions in that cuboid
   * @param sizeArray Array storing, for each cuboid stored in the file, the number of cells in that cuboid (non-zero cells for SparseCuboid)
   * @return Map from projection ID to Cuboid identifier
   */
  def readMultiCuboid(filename: String, idArray: Array[Int], isSparseArray: Array[Boolean], nbitsArray: Array[Int], sizeArray: Array[Int]): Map[Int, Cuboid]

  /**
   * Writes a file storing the data from multiple cuboids in memory
   * @param filename Name of the file
   * @param cuboidsArray Array storing the identifiers of Cuboids to be written to this file
   */
  def writeMultiCuboid(filename: String, cuboidsArray: Array[Cuboid]): Unit

  /**
   * Saves the contents of given cuboids using a Trie that stores its moments
   * Experimental feature only in CBackend
   * @param cuboids Array storing, for each cuboid that is to be stored in the trie, the dimensions in that cuboid
   *                (encoded as Int) as well as identifier to the Cuboid
   * @param filename Name of file
   * @param maxSize Maximum node size in the trie. Once the trie capapcity is reached, no additional moments are stored
   */
  def saveAsTrie(cuboids: Array[(Array[Int], HYBRID_T)], filename: String, maxSize: Long): Unit

  /**
   * Loads Trie representation of Cuboids from a file into memory
   * Experimental feature only in CBackend
   * @param filename Name of the file
   */
  def loadTrie(filename: String): Unit
  /**
   * Finds moments relevant to a given query from the trie storing moments of several cuboids
   * @return Map containing the value of the moment for the available projections of the query (normalized and encoded using Int)
   *         TODO: Change to MEASURE_T
   *         @see [[core.solver.SolverTools.preparePrimaryMomentsForQuery]]
   */
  def prepareFromTrie(query: IndexedSeq[Int]) : Seq[(Int, Long)]

  /**
   * Initializes a base cuboid with streamed data
   *
   * @param n_bits Number of dimensions for base cuboid
   * @param it Iterator to Key-Value pairs storing the cell-address (as BigBinary) and cell value as Long
   *           TODO: Change cell value type to MEASURES_T
   * @return The base cuboid encoded as a [[SparseCuboid]]
   */
  def mk(n_bits: Int, it: Iterator[(BigBinary, Long)]) : SparseCuboid


  /**
   * Initializes a base cuboid with given data. Faster than [[mk]] because the entire data is passed
   * TODO: Change to MEASURES_T
   * @param n_bits Number of dimensions for base cuboid
   * @param kvs Sequence of key-value pairs storing cell address and cell values
   * @return Base cuboid encoded as [[SparseCuboid]]
   */
  def mkAll(n_bits: Int, kvs: Seq[(BigBinary, Long)]) : SparseCuboid

  /**
   * Adds data to an existing base cuboid initialized using [[initPartial]]
   * @param n_bits The current number of dimensions for base cuboid
   * @param it Data to be added
   * @param sc Existing base cuboid
   * @return Updated base cuboid
   */
  def addPartial(n_bits: Int, it: Iterator[(BigBinary, Long)], sc : SparseCuboid): SparseCuboid
  /**
   * Initializes a base cuboid for which the data will be added using several calls to [[addPartial]]
   * @return Empty Sparse Cuboid
   */
  def initPartial(): SparseCuboid
  /**
   * Finalizes the base cuboid initialized by [[initPartial]]. May involve change in representation in the backend
   * @param sc Base Cuboid
   * @return
   */
  def finalisePartial(sc :SparseCuboid): SparseCuboid

  /**
   * Fetch cell values from a [[DenseCuboid]] as an array.
   * @param data Identifier to Dense Cuboid
   * @return Array containing values of the cuboid
   */
  protected def dFetch(data: DENSE_T) : Array[MEASURES_T]

  /**
   * Unload cuboid from memory.
   * Experimental feature used in CBackend
   */
  protected def cuboidGC(data: HYBRID_T): Unit

  /**
   * Number of non-zero cells of a SparseCuboid
   */
  protected def sSize(data: SPARSE_T) : BigInt

  /**
   * Number of bytes for the storage of a SparseCuboid. Calculated as size * (keySize + valueSize)
   */
  protected def sNumBytes(data: SPARSE_T) : Long

  /** Smart rehash of a Sparse Cuboid to either Sparse or Dense cuboid */
  protected def hybridRehash(a: SPARSE_T,  bitpos: BITPOS_T ) : HYBRID_T
  /** Project a Dense Cuboid to a Sparse Cuboid */
  protected def d2sRehash(n_bits: Int, a: DENSE_T,  bitpos: BITPOS_T) : SPARSE_T
  /** Project a Sparse Cuboid to a Dense Cuboid */
  protected def s2dRehash(a: SPARSE_T, p_bits: Int, bitpos: BITPOS_T) : DENSE_T
  /**Project Sparse Cuboid to another Sparse Cuboid*/
  protected def   sRehash(a: SPARSE_T,              bitpos: BITPOS_T) : SPARSE_T
  /** Project Dense Cuboid to another Dense Cuboid */
  protected def   dRehash(n_bits: Int, a: DENSE_T, p_bits: Int,
                          bitpos: BITPOS_T) : DENSE_T

  /**
   * Stores non-zero cells as a sequence of key-value pairs. Key is cell address and Value is fact value in the cell
   */
  case class SparseCuboid(
    n_bits: Int,
    /* private */ val data: SPARSE_T
  ) extends Cuboid {

    def size = sSize(data)
    def numBytes: Long = sNumBytes(data)
    override def gc = {
      cuboidGC(sparseToHybrid(data))
    }
    override def rehash(bitpos: BITPOS_T): Cuboid = {
      val h = hybridRehash(data, bitpos)
      if(isDense(h))
        DenseCuboid(bitpos.length, extractDense(h))
      else
        SparseCuboid(bitpos.length, extractSparse(h))
    }

    def rehash_to_dense(bitpos: BITPOS_T) = {
      val res_n_bits = bitpos.length
      DenseCuboid(res_n_bits, s2dRehash(data, res_n_bits, bitpos))
    }

    def rehash_to_sparse(bitpos: BITPOS_T) = {
      SparseCuboid(bitpos.length, sRehash(data, bitpos))
    }

    def backend = be_this
  }

  /**
   * Stores cell values in a large array
  */
  case class DenseCuboid(
    n_bits: Int,
    /* private */ val data: DENSE_T
  ) extends Cuboid {

    def size = Big.pow2(n_bits)
    override def numBytes: Long = (size * 8).toLong

    /** smart rehash */
    override def rehash(bitpos: BITPOS_T): Cuboid = rehash_to_dense(bitpos)

    def rehash_to_dense(bitpos: BITPOS_T) = {
      val res_n_bits = bitpos.length
      DenseCuboid(res_n_bits, dRehash(n_bits, data, res_n_bits, bitpos))
    }

    def rehash_to_sparse(bitpos: BITPOS_T) = {
      SparseCuboid(bitpos.length, d2sRehash(n_bits, data, bitpos))
    }

    /** Returns the contents of cuboid as an array. Only available for [[DenseCuboid]] */
    def fetch: Array[MEASURES_T] = dFetch(data)
    override def gc: Unit = cuboidGC(denseToHybrid(data))
    def backend = be_this
  }
}


