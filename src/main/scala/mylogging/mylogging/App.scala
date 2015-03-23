package mylogging.mylogging
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils


// spark-submit --class LogAnalysis --master yarn-client SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err

// no limit on parallel jobs, but stdout is not displayed locally: need to look into worker log
// spark-submit --class LogAnalysis --master yarn-cluster SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err




/*
// spark-shell --master yarn-client --num-executors 25


import scala.util.parsing.combinator._


object Adder extends RegexParsers {
  def expr: Parser[Int] = (
   "("~expr~"+"~expr~")" ^^ { case "("~x~"+"~y~")" => x+y }
  | number
  )

  val number: Parser[Int] = """[0-9]+""".r ^^ { _.toInt}
}

Adder.parse(Adder.expr, "(7+4)")


object LogP extends RegexParsers {
  def logline: Parser[Any] = (
    timestamp~"""INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt"""~ident~"is done. finalState="~ident ^^ {
       case t~_~a~_~s => Option(t, a, s)
    }
  |  "[^\n]+".r ^^ (_ => None)
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
}


val l1 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt appattempt_1425682538854_1748_000001 is done. finalState=FINISHED"""

val l2 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1425682538854_1748,name=decompress,user=joe,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1425682538854_1748/jobhistory/job/job_1425682538854_1748,appMasterHost=icdatasrv019.icdatacluster2,startTime=1425960925158,finishTime=1425960946325,finalStatus=FAILED"""

val l3 = """2015-03-03 17:59:51,137 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1424932957480_0341,name=Spark shell,user=bob,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1424932957480_0341/A,appMasterHost=icdatasrv058.icdatacluster2,startTime=1425392421737,finishTime=1425401990053,finalStatus=SUCCEEDED"""

*/



import scala.util.parsing.combinator._


abstract class LogLine extends java.io.Serializable
case class Foo(ts: String, appatt: String, state: String) extends LogLine
case class AppSummary(timestamp: String, app: String, name: String, user: String, state:String,
  url:String, host: String, startTime: String, endTime: String, finalStatus: String) extends LogLine
case class UnknownLine() extends LogLine


// Yarn Log Parser
object LogP extends RegexParsers with java.io.Serializable {
  def logline: Parser[LogLine] = (
    timestamp
        ~"INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt"~ident
        ~"is done. finalState="~ident ^^ {
       case t~_~a~_~s => Foo(t, a, s)
    }
  | timestamp~"INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId="~ident
        ~",name="~identW
        ~",user="~ident
        ~",queue=default,state="~ident
        ~",trackingUrl="~url
        ~",appMasterHost="~ident
        ~".icdatacluster2,startTime="~ident
        ~",finishTime="~ident
        ~",finalStatus="~ident ^^ {
       case t~_~app~_~name~_~user~_~state~_~url~_~host~_~stime~_~etime~_~finalStatus =>
         AppSummary(t, app, name, user, state, url, host, stime, etime, finalStatus)
    }
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
}



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogAnalysis1 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("LogAnalysis"))
    val lines = sc.textFile("/Users/alex/workspace_scala/Homework2/yarn-yarn-resourcemanager-master1.log*")


    def parseLine(l: String): LogLine =
      LogP.parse(LogP.logline, l).getOrElse(UnknownLine())
       

/*
    parseLine(l2)
*/

    def f(a: LogLine) = a match {
      case AppSummary(t, app, name, user, state, url, host, stime, etime, finalStatus) => List(user+" "+state+" "+host+" "+stime+" "+etime+" "+finalStatus)
      case _ => List()
    }
    def ftemp(a: LogLine) = a match {
      case AppSummary(t, app, name, user, state, url, host, stime, etime, finalStatus) => if (finalStatus == "SUCCEEDED") List("1 1:" + (etime.toDouble-stime.toDouble).toString) else List("0 1:" + (etime.toDouble-stime.toDouble).toString)
      case _ => List()
    }
    def fother(a: LogLine) = a match {
      case Foo(t, app, user) => List(t+" "+app+" "+user)
      case _ => List()
    }

    val ll = lines.map(l => parseLine(l)).flatMap(f).cache
    ll.saveAsTextFile("app_summaries_one")  // in the user's HDFS home directory

    val ll2 = lines.map(l => parseLine(l)).flatMap(ftemp).cache
    ll2.saveAsTextFile("app_summaries_three")  // in the user's HDFS home directory
    
    sc.stop()
  }
}




