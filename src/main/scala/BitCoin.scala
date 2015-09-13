import akka.actor._
import akka.routing.{RoundRobinPool, RoundRobinRouter}
import sun.plugin.navig.motif.Worker

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.util.Random

/**
 * Created by sarathfrancis90 on 9/5/15.
 */
object BitCoin {




  sealed trait bitCoin
  case class StartMining(RandomStringList: ListBuffer[String]) extends bitCoin
  case object ContinueMining extends bitCoin
  case object BitCoinMining extends bitCoin
  case object Result extends  bitCoin
  case object StopMining extends bitCoin

  def SHA256(s: String): String = {
    // Besides "MD5", "SHA-256", and other hashes are available
    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
  }

  def createRandomString(stringLength:Int):String ={

    val randomString = Random.alphanumeric.take(stringLength).mkString
    "sarathfrancis90;" + randomString
  }

  def NonZerochar(c: Char): Boolean = c != '0'

  def checkLeadingZeros(hashedCoin : String,leadingZeros: Int):Boolean = {


    if(hashedCoin.indexWhere(NonZerochar)>=leadingZeros)
      {
        //println("success")
        true
      }

    else {
      //println("failure")
      false

    }
  }

    class Worker extends Actor with ActorLogging {

    var masterRef:ActorRef=_

       def receive  = {

        //Worker receives message from Master to start Mining the coins in the set of 80
       case StartMining(randomStringList) =>

         //saving reference of the Master
         masterRef = sender()



         self ! ContinueMining

       case ContinueMining =>

         val noOfZeros = 4
         //log.info(s"received the message from Master")
         //val randomGeneratedString = createRandomString(10)
         //println(randomGeneratedString)
         val hashedCoin = SHA256(randomGeneratedString)

          if (checkLeadingZeros(hashedCoin, noOfZeros)) {
            // println( "sending to master")
              println(randomGeneratedString +"  " + hashedCoin)
             masterRef ! Result

          }
          else
          {
           self ! ContinueMining
           }

     }
  }


  class Master extends Actor with ActorLogging {

    import context._

    val noOfZeros = 4
    val noOfWorkers: Int = 4
    var randomStringList: ListBuffer[String] = _

    def receive = {

      case Result =>
        //log.info("Final value from the worker")
        sender ! StartMining

      case BitCoinMining =>

        log.info("Starting bitCoin Mining")
        //creating RoundRobinPool for managing Worker
        val workerRouter:ActorRef = context.actorOf(RoundRobinPool(noOfWorkers).props(Props[Worker]),name = "workerRouter")


        //Generate Random String prefixed with the GatorID and append into a list buffer
        for(i <- 0 until 80) randomStringList+= createRandomString(10)
        randomStringList.toList
        //Sending the batch of 80 strings to the Workers in the roundrobin fashion
        for( i<- 0 until  noOfWorkers) workerRouter ! StartMining(randomStringList)

        val totalTimeDuration = Duration(100000,"millis")
        context.system.scheduler.scheduleOnce(totalTimeDuration, self,StopMining)

      case StopMining =>

        log.info("Mining stopped")
        context.system.shutdown()
    }



  }
  def main (args: Array[String]) {

      val system = ActorSystem("BitCoinMining")

      val master = system.actorOf(Props(new Master),name = "Master")



      master ! BitCoinMining

      system.awaitTermination()

  }
}