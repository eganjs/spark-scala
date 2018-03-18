package eganjs

object Main {
  def main(args: Array[String]): Unit = {
    val job = new PrepareDataJob("Get and decode data")
    job.run()
  }
}
