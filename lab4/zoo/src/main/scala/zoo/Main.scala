package zoo

import scala.util.Random

object Main {
  val sleepTime = 100

  def main(args: Array[String]): Unit = {
    println("Starting animal runner")

//    val Seq(animalName, hostPort, partySize) = args.toSeq
    val animalName = "aboba"
    val hostPort = "127.0.0.1:2181"
    val partySize = "1"
    val animal = Animal(animalName, hostPort, "/zoo", partySize.toInt)
    try {
      animal.enter()
      println(s"${animal.name} entered.")
      for (i <- 1 to Random.nextInt(100)) {
        Thread.sleep(sleepTime)
        println(s"${animal.name} is running...")
      }

      animal.leave()
    } catch {
      case e: Exception => println("Animal was not permitted to the zoo. " + e)
    }
  }
}
