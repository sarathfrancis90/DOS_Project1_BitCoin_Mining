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
  case class Result(MinedCoinList: List[String]) extends  bitCoin
  case class ResultfromRemote(RemotelyMinedCoinList: List[String]) extends  bitCoin
  case object StopMining extends bitCoin
  case class StartMiningfromMainServer(remoteStringList: List[String],noOfZeros:Int)

  def SHA256(s: String): String =
  {
    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
  }
  def createRandomString(stringLength:Int):String =
  {
    val randomString = Random.alphanumeric.take(stringLength).mkString
    "sarathfrancis90;" + randomString
  }

  def NonZerochar(c: Char): Boolean = c != '0'

  def checkLeadingZeros(hashedCoin : String,leadingZeros: Int):Boolean = {

    if(hashedCoin.indexWhere(NonZerochar)>=leadingZeros)
      {
       true
      }

    else
    {
     false

    }
  }

    class Worker extends Actor with ActorLogging {

      var masterRef: ActorRef = _
      var finalMinedCoinList:  ListBuffer[String] = new ListBuffer[String]()

      def receive = {

        //Init message from master
        case WorkerInit =>
          //accepting the init meesage from the master and worker is ready to work
          sender ! Readytowork

        //Worker receives message from Master to start Mining the coins in the set of 80
        case StartMining(randomStringList, noOfZeros) =>
          //for(i<-randomStringList.indices)log.info(randomStringList(i))

          //iterating through the random string list
          for (i <- randomStringList.indices)
          {
            val hashedCoin: String = SHA256(randomStringList(i))
            //log.info(randomStringList(i) + " " + hashedCoin)
            if (checkLeadingZeros(hashedCoin, noOfZeros))
            {
              //sending hashed coins with required number of prefixed zeros to master
              val finalMinedCoin:String = randomStringList(i) + "  " + hashedCoin
              finalMinedCoinList+=finalMinedCoin
            }
          }
          val FinalList = finalMinedCoinList.toList
          sender ! Result(FinalList)
          finalMinedCoinList.clear()
      }
    }

  class Master extends Actor with ActorLogging {

    import context._

    var no_OfZeros: Int =_ //variable to store number of prefixed zeros
    val noOfWorkers: Int = 4 //Number of worker Actors
    var randomStringList:  ListBuffer[String] = new ListBuffer[String]() //List for storing strings generated strings in the main Server
    var remoteRandomStringList:  ListBuffer[String] = new ListBuffer[String]()  ////List for storing strings generated strings in the remote Server
    var workerRouter : ActorRef =_  //Round Robin Router for work allocation in the main server
    var remoteWorkerRouter: ActorRef =_  //Round Robin Router for work allocation in the main server
    var MainServerActorRef :ActorSelection =_ // variable for storing the main master reference in the remote master
    var masterRole : Int = 0; // flag to check which master is sending messages 0 means main master
    var localRequests: Int=0   // flag to check if all the workers in the remote completed mining
    var bigFinalList: ListBuffer[String] = new ListBuffer[String]()  //llst to store the work of all workers in the remote and combine them and send to the main master
    var serverResponseWaitCancellable: Cancellable = _
    def receive = {
      //Initiating Main master from the main
      case MasterInit(noOfZeros) =>
        masterRole = 1
        no_OfZeros=noOfZeros
        //setting the timeDuration for execution. Schedule Stopmining message after a specific time to stop mining
        val totalTimeDuration = Duration(30000, "millis")
        context.system.scheduler.scheduleOnce(totalTimeDuration, self, StopMining)

        //creating RoundRobinPool for managing Worker
        workerRouter = context.actorOf(RoundRobinPool(noOfWorkers).props(Props[Worker]), name = "workerRouter")
        //Initiating all the workers
        for (i <- 0 until noOfWorkers) workerRouter ! WorkerInit

       //Result from local workers
      case Result(finalList) =>
        //Final value from the worker
        if (masterRole == 1)
        {
          finalList.foreach(println)
          self ! BitCoinMining
        }
        else
        {
          localRequests -=1
          if(localRequests == 0)
          {
            MainServerActorRef ! ResultfromRemote(bigFinalList.toList)
            MainServerActorRef ! RemoteMasterReadytoWork
            val totalTimeDuration = Duration(3000, "millis")
            serverResponseWaitCancellable = system.scheduler.scheduleOnce(totalTimeDuration, self, StopMining)
            bigFinalList.clear()
          }
          else
          {
            finalList.foreach(bigFinalList += _)
            finalList.foreach(println(_))

          }
        }

      //print the combined result from the remote Master
      case ResultfromRemote(remotefinalList)  =>
        remotefinalList.foreach(println)

      //self message from the workers after the  Init
      case Readytowork =>
        if(masterRole ==1) self ! BitCoinMining

       // Initiating remote Master with the Main server Ip Address
      case RemoteMasterInit(serverIp) =>
        println("RemoteMasterInitiated")
        MainServerActorRef = context.actorSelection("akka.tcp://BitCoinMining@" + serverIp + ":2552/user/Master")
        remoteWorkerRouter = context.actorOf(RoundRobinPool(noOfWorkers).props(Props[Worker]),name = "workerRouter")
        for( i<- 0 until  noOfWorkers) remoteWorkerRouter ! WorkerInit
        MainServerActorRef ! RemoteMasterReadytoWork

      //message from remote server telling that it is ready
      case RemoteMasterReadytoWork =>
        for(i <- 0 until 800) remoteRandomStringList += createRandomString(12)
        val StringList = remoteRandomStringList.toList
        sender ! StartMiningfromMainServer(StringList,no_OfZeros)
        remoteRandomStringList.clear()

      //Message from Main Server to start Mining
      case StartMiningfromMainServer(remoteStringList,noOfZeros) =>
        val remoteRandomStringsdividedtogroups = remoteStringList.toList.grouped(remoteStringList.length/noOfWorkers).toList
        for( i<- 0 until  noOfWorkers){
          remoteWorkerRouter ! StartMining(remoteRandomStringsdividedtogroups(i),noOfZeros)
          localRequests+=1
        }

      //Message to start Mining
      case BitCoinMining =>
        //Generate Random String prefixed with the GatorID and append into a list buffer
        for(i <- 0 until 200) randomStringList += createRandomString(12)
        val RandomStringsdividedtogroups = randomStringList.toList.grouped(randomStringList.length/noOfWorkers).toList
        //Sending the batch of 50 strings to the Workers in the round-robin fashion
        for( i<- 0 until  noOfWorkers) workerRouter ! StartMining(RandomStringsdividedtogroups(i),no_OfZeros)
        randomStringList.clear()


      //Stop Mining Message to shutdown system
      case StopMining =>
        println("Mining stopped")
        context.system.shutdown()
    }

  }
  def main (args: Array[String]) {

      //Creating Actor System
      val system = ActorSystem("BitCoinMining")

      //Creating Master Actor
      val master = system.actorOf(Props(new Master),name = "Master")

      //Initiating Remote Master based on the command line arguments
      if(args(0).mkString.contains(".")) master ! RemoteMasterInit(args(0))
      else master ! MasterInit(args(0).toInt)


      system.awaitTermination()

  }
}
