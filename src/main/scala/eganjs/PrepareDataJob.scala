package eganjs

import java.io.InputStream
import java.net.URL
import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.GZIPInputStream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class PrepareDataJob(jobName: String) extends SparkJob(jobName) {

  private val labelsFileType = 2049
  private val imagesFileType = 2051

  type DatasetMetadata = (String, String)
  private val datasetPairs: Array[DatasetMetadata] = Array(
    ("http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz"),
    ("http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz", "http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz")
  )

  override protected def job(sc: SparkContext): Unit = {

    val datasets: RDD[(Long, DatasetMetadata)] = sc
      .parallelize(datasetPairs, numSlices = 4)
      .zipWithUniqueId()
      .map(_.swap)

    val fileUrls: RDD[(Long, String)] = datasets.flatMapValues { case (v1, v2) => Seq(v1, v2) }

    val fileDataStreams: RDD[(Long, Iterator[Byte])] = fileUrls.mapValues(readToStream _ andThen decodeGzippedStream)

    val labelsFileDataStreams: RDD[(Long, Iterator[Byte])] = fileDataStreams.filter(isNextIntEqualTo(labelsFileType))
    val imagesFileDataStreams: RDD[(Long, Iterator[Byte])] = fileDataStreams.filter(isNextIntEqualTo(imagesFileType))

    val labelsCount: RDD[(Long, (String, Int))] = labelsFileDataStreams
      .mapValues { data =>
        val labelsCount = readNextInt(data)
        ("L", labelsCount)
      }

    val imagesCount: RDD[(Long, (String, Int))] = imagesFileDataStreams
      .mapValues { data =>
        val imageCount = readNextInt(data)
        val imageHeight = readNextInt(data)
        val imageWidth = readNextInt(data)
        ("I", imageCount)
      }

    val counts = labelsCount.zip(imagesCount)

    counts.toLocalIterator.foreach(println(_))
  }

  private def isNextIntEqualTo(next: Int): ((_, Iterator[Byte])) => Boolean = {
    case (_, data) => readNextInt(data) == next
  }

  private def readNextInt(bytes: Iterator[Byte]): Int = {
    ByteBuffer
      .wrap {
        bytes
          .take(4)
          .grouped(4)
          .next()
          .toArray
      }
      .order(ByteOrder.BIG_ENDIAN)
      .getInt
  }

  private def readToStream(url: String): InputStream = {
    new URL(url).openStream()
  }

  private def decodeGzippedStream(is: InputStream): Iterator[Byte] = {
    val gZIPInputStream = new GZIPInputStream(is)
    Stream
      .continually(gZIPInputStream.read())
      .takeWhile(_ != -1)
      .map(_.toByte)
      .toIterator
  }
}
