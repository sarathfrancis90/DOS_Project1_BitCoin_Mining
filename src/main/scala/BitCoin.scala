import akka.actor._
import akka.routing.{RoundRobinPool, RoundRobinRouter}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.util.Random

/**
 * Created by sarathfrancis90 on 9/5/15.
 */
object BitCoin {




  sealed trait bitCoin
  case class StartMining(RandomStringList: List[String],noOfZeros: Int) extends bitCoin
  case object ContinueMining extends bitCoin
  case class MasterInit(noOfZeros: Int) extends bitCoin
  case class RemoteMasterInit(ServerIp: String) extends bitCoin
  case object RemoteMasterReadytoWork extends bitCoin
  case object WorkerInit extends bitCoin
  case object Readytowork extends bitCoin
  case object BitCoinMining extends bitCoin
  case class Result(RandomStringList: List[String]) extends  bitCoin
  case object StopMining extends bitCoin
  case class StartMiningfromMainServer(remoteStringList: List[String],noOfZeros:Int)

  def SHA256(s: String): String = {
    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
  }
  def createRandomString(stringLength:Int):String ={
    val randomString = Random.alphanumeric.take(stringLength).mkString
    //println(randomString)
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

      var masterRef: ActorRef = _
      var finalMinedCoinList:  ListBuffer[String] = new ListBuffer[String]()

      def receive = {

        //Init message from master
        case WorkerInit =>
          log.info("Worker initiated")
          //accepting the init meesage from the master and worker is ready to work
          sender ! Readytowork

        //Worker receives message from Master to start Mining the coins in the set of 80
        case StartMining(randomStringList, noOfZeros) =>
          //for(i<-randomStringList.indices)log.info(randomStringList(i))

          //iterating through the random string list
          for (i <- randomStringList.indices) {
            //log.info(randomStringList(i))
            //println(randomString)
            val hashedCoin: String = SHA256(randomStringList(i))
            //log.info(randomStringList(i) + " " + hashedCoin)
            if (checkLeadingZeros(hashedCoin, noOfZeros)) {
              //sending hashed coins with required number of prefixed zeros to master
              val finalMinedCoin:String = randomStringList(i) + "  " + hashedCoin
              //log.info(finalMinedCoin)
              finalMinedCoinList+=finalMinedCoin
             //masterRef !Readytowork
            }
          }
          val FinalList = finalMinedCoinList.toList
          sender ! Result(FinalList)
          finalMinedCoinList.clear()
      }
    }

  class Master extends Actor with ActorLogging {

    import context._

    var no_OfZeros: Int =_
    val noOfWorkers: Int = 4
    var randomStringList:  ListBuffer[String] = new ListBuffer[String]()
    var remoteRandomStringList:  ListBuffer[String] = new ListBuffer[String]()
    var workerRouter : ActorRef =_
    var remoteWorkerRouter: ActorRef =_
    var MainServerActorRef :ActorSelection =_
    var masterRole : Int = 0;
    var onlyOnce: Int = 0
    var localRequests: Int=0
    var bigFinalList: ListBuffer[String] = new ListBuffer[String]()

    def receive = {

      case MasterInit(noOfZeros) =>
        masterRole = 1
        no_OfZeros=noOfZeros
        log.info("Master Initiated")
        val totalTimeDuration = Duration(30000, "millis")
        context.system.scheduler.scheduleOnce(totalTimeDuration, self, StopMining)

        //creating RoundRobinPool for managing Worker
        workerRouter = context.actorOf(RoundRobinPool(noOfWorkers).props(Props[Worker]), name = "workerRouter")

        for (i <- 0 until noOfWorkers) workerRouter ! WorkerInit

      case Result(finalList) =>
        //Final value from the worker
        if (masterRole == 1) {
        //finalList.foreach(println)
        self ! BitCoinMining
        }
        else
          {
            localRequests -=1
            if(localRequests == 0)

              {
                val justbeforesending = bigFinalList.toList
                justbeforesending.foreach(println(_))
                MainServerActorRef ! Result(bigFinalList.toList)
                MainServerActorRef ! RemoteMasterReadytoWork
                bigFinalList.clear()
              }
            else
              {

                finalList.foreach(bigFinalList += _)
                finalList.foreach(println(_))

              }
          }



      case Readytowork =>
        if(masterRole ==1) self ! BitCoinMining


      case RemoteMasterInit(serverIp) =>


        log.info("RemoteMasterInitiated")
        MainServerActorRef = context.actorSelection("akka.tcp://BitCoinMining@" + serverIp + ":2552/user/Master")
        remoteWorkerRouter = context.actorOf(RoundRobinPool(noOfWorkers).props(Props[Worker]),name = "workerRouter")
        for( i<- 0 until  noOfWorkers) remoteWorkerRouter ! WorkerInit

         MainServerActorRef ! RemoteMasterReadytoWork

      case RemoteMasterReadytoWork =>
        println("remote master ready")
        for(i <- 0 until 800) remoteRandomStringList += createRandomString(12)
        val StringList = remoteRandomStringList.toList
        //for( i<- randomStringList.indices)println(randomStringList(i))
        sender ! StartMiningfromMainServer(StringList,no_OfZeros)
        remoteRandomStringList.clear()

      case StartMiningfromMainServer(remoteStringList,noOfZeros) =>
        val remoteRandomStringsdividedtogroups = remoteStringList.toList.grouped(remoteStringList.length/noOfWorkers).toList
        for( i<- 0 until  noOfWorkers){
          remoteWorkerRouter ! StartMining(remoteRandomStringsdividedtogroups(i),noOfZeros)
          localRequests+=1
        }


      case BitCoinMining =>
        //Generate Random String prefixed with the GatorID and append into a list buffer
        for(i <- 0 until 200) randomStringList += createRandomString(12)
        //for( i<- randomStringList.indices)println(randomStringList(i))
        val RandomStringsdividedtogroups = randomStringList.toList.grouped(randomStringList.length/noOfWorkers).toList
        //Sending the batch of 50 strings to the Workers in the round-robin fashion
        for( i<- 0 until  noOfWorkers) workerRouter ! StartMining(RandomStringsdividedtogroups(i),no_OfZeros)

        randomStringList.clear()



      case StopMining =>
        log.info("Mining stopped")
        context.system.shutdown()
    }

  }
  def main (args: Array[String]) {

      val system = ActorSystem("BitCoinMining")

      val master = system.actorOf(Props(new Master),name = "Master")

      if(args(0).mkString.contains(".")) master ! RemoteMasterInit(args(0))
      else master ! MasterInit(args(0).toInt)
      //master ! BitCoinMining

      system.awaitTermination()

  }
}
