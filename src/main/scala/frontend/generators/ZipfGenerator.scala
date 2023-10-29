package frontend.generators
import backend.CBackend
import frontend.schema._
import util.BigBinary
import com.github.tototoshi.csv.CSVReader
import frontend.schema.encoders.{StaticMemCol, StaticNatCol}

class ZipfGenerator(alpha:Double = 1.1)(implicit backend: CBackend) extends CubeGenerator("ZipfGenerator")  {
  override lazy val schemaInstance = schema()
  override val measureName: String = "Value"

  override def generatePartitions(): IndexedSeq[(Int, Iterator[(BigBinary, Long)])] = {
    val filename = s"tabledata/zipf/data_dim=21_alpha=${alpha}.csv"
    val datasize = CSVReader.open(filename).iterator.drop(1).size
    val data = CSVReader.open(filename).iterator.drop(1).map { s =>
      val sIdx = s.toIndexedSeq
      val keys = sIdx.dropRight(1).reverse //for the left-most column to be assigned higher bits
      val value = sIdx.last
      val encodedKey = schemaInstance.encode_tuple(keys)
      val encodedValue = StaticNatCol.defaultToInt(value).get.toLong
      //StaticNatCol.floatToInt(2)(value).get.toLong
      encodedKey -> encodedValue
    }
    Vector(datasize -> data)
  }

  override protected def schema(): StaticSchema2 = {
    val bit0 = new LD2("Bit_0", new StaticMemCol[Int](1, Vector(0,1)))
    val bit1 = new LD2("Bit_1", new StaticMemCol[Int](1, Vector(0,1)))
    val bit2 = new LD2("Bit_2", new StaticMemCol[Int](1, Vector(0,1)))
    val bit3 = new LD2("Bit_3", new StaticMemCol[Int](1, Vector(0,1)))
    val bit4 = new LD2("Bit_4", new StaticMemCol[Int](1, Vector(0,1)))
    val bit5 = new LD2("Bit_5", new StaticMemCol[Int](1, Vector(0,1)))
    val bit6 = new LD2("Bit_6", new StaticMemCol[Int](1, Vector(0,1)))
    val bit7 = new LD2("Bit_7", new StaticMemCol[Int](1, Vector(0,1)))
    val bit8 = new LD2("Bit_8", new StaticMemCol[Int](1, Vector(0,1)))
    val bit9 = new LD2("Bit_9", new StaticMemCol[Int](1, Vector(0,1)))
    val bit10 = new LD2("Bit_10", new StaticMemCol[Int](1, Vector(0,1)))
    val bit11 = new LD2("Bit_11", new StaticMemCol[Int](1, Vector(0,1)))
    val bit12 = new LD2("Bit_12", new StaticMemCol[Int](1, Vector(0,1)))
    val bit13 = new LD2("Bit_13", new StaticMemCol[Int](1, Vector(0,1)))
    val bit14 = new LD2("Bit_14", new StaticMemCol[Int](1, Vector(0,1)))
    val bit15 = new LD2("Bit_15", new StaticMemCol[Int](1, Vector(0,1)))
    val bit16 = new LD2("Bit_16", new StaticMemCol[Int](1, Vector(0,1)))
    val bit17 = new LD2("Bit_17", new StaticMemCol[Int](1, Vector(0,1)))
    new StaticSchema2(Vector(bit0,bit1,bit2,bit3,bit4,bit5,bit6,bit7,bit8,bit9,bit10,bit11,bit12,bit13,bit14,bit15,bit16,bit17))
  }
}
object ZipfGenerator {
  def main(args: Array[String]) {
    implicit val be = CBackend.default
    val cg = new ZipfGenerator()
    cg.saveBase()
  }
}

