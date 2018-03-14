package eganjs

import org.apache.spark.{SparkConf, SparkContext}

abstract class SparkJob(protected val name: String, private val master: String = "local[*]") extends Serializable {

  final def run(): Unit = {
    val sc = SparkContext.getOrCreate {
      new SparkConf()
        .setAppName(name)
        .setMaster(master)
    }
    job(sc)
    sc.stop()
  }

  protected def job(sc: SparkContext): Unit

}
