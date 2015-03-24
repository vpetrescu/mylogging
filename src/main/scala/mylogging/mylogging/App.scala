package mylogging.mylogging
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import scala.util.parsing.combinator._

// spark-submit --class LogAnalysis --master yarn-client SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err

// no limit on parallel jobs, but stdout is not displayed locally: need to look into worker log
// spark-submit --class LogAnalysis --master yarn-cluster SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err

/*
// spark-shell --master yarn-client --num-executors 25
put -r /Users/alex/workspace_scala/mylogging/src/main/scala/mylogging/mylogging/App.scala

import scala.util.parsing.combinator._

val l1 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt appattempt_1425682538854_1748_000001 is done. finalState=FINISHED"""

val l2 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1425682538854_1748,name=decompress,user=joe,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1425682538854_1748/jobhistory/job/job_1425682538854_1748,appMasterHost=icdatasrv019.icdatacluster2,startTime=1425960925158,finishTime=1425960946325,finalStatus=FAILED"""

val l3 = """2015-03-03 17:59:51,137 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1424932957480_0341,name=Spark shell,user=bob,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1424932957480_0341/A,appMasterHost=icdatasrv058.icdatacluster2,startTime=1425392421737,finishTime=1425401990053,finalStatus=SUCCEEDED"""
val l4 = """2015-03-10 13:44:55,243 INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl: Memory usage of ProcessTree 227772 for container-id container_1425682538854_2409_01_000289: 2.4 GB of 8.7 GB physical memory used; 8.0 GB of 18.2 GB virtual memory used"""
*/


abstract class LogLine extends java.io.Serializable
case class Foo(ts: String, appatt: String, state: String) extends LogLine
case class AppSummary(timestamp: String, app: String, name: String, user: String, state:String,
  url:String, host: String, startTime: String, endTime: String, finalStatus: String) extends LogLine
case class AppContainer(container_id: String, app:String) extends LogLine
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
   | timestamp
        ~"INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl: Memory usage of ProcessTree"~processtree
        ~"for container-id "~container_part
        ~":"~other ^^ {
           case t~_~processtree~_~containernbr~_~other => AppContainer(containernbr, "application"+ containernbr.drop(9).dropRight(10))
         }
  )

  val container_part = "container_[0-9]+_[0-9]+_[0-9]+_[0-9]+".r
  val processtree: Parser[String] = "[0-9]+".r
  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val other: Parser[String] = "[a-zA-Z0-9.;]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogAnalysis1 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("LogAnalysis"))
   
    val lines = sc.textFile("/Users/alex/workspace_scala/clusterlogs/*")

    def parseLine(l: String): LogLine =
      LogP.parse(LogP.logline, l).getOrElse(UnknownLine())
       

    def f(a: LogLine) = a match {
      case AppSummary(t, app, name, user, state, url, host, stime, etime, finalStatus) => List((app,finalStatus))
      case _ => List()
    }

    def fnodemagenerLines(a: LogLine) = a match {
      case AppContainer(container, app) => List(List(app, container))
      case _ => List()
    }

    val ll = lines.map(l => parseLine(l)).flatMap(fnodemagenerLines).cache
    var lines_unique_containers = ll.groupBy(l=> l(0)).mapValues(listofpairs => listofpairs.toSet).mapValues(x => x.size)    
    var lines_max_container = ll.map(x => (x(0),x(1)) ).groupBy(l=>l).map(l=> (l._1._1, l._2.size)).groupBy(l=> l._1).mapValues(mlist=> mlist.map(x=>x._2)).mapValues(x=>x.max)
    
    var llresource = lines.map(l=>parseLine(l)).flatMap(f)
    

    llresource.saveAsTextFile("app_resource_sum")
    lines_max_container.saveAsTextFile("app_summaries_one")
    val joinoutput = llresource.join(lines_max_container)
    joinoutput.saveAsTextFile("joinoutput")
 
    // Add the running time of the application to the SVM
    def fsvm(a: LogLine) = a match {
      case AppSummary(t, app, name, user, state, url, host, stime, etime, finalStatus) => if (finalStatus == "SUCCEEDED") List("1 " + (etime.toDouble-stime.toDouble).toString) else List("0 " + (etime.toDouble-stime.toDouble).toString)
      case _ => List()
    }
    val linessvm = lines.map(l => parseLine(l)).flatMap(fsvm).cache
    val svmdata = linessvm.map { line =>
      val parts = line.split(" ").map(_.toDouble)
        LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }
    /*
    val CVfold:Int = 2
    var auROC:Array[Double] = new Array[Double](CVfold)
    for (cv_iter <- 0 to CVfold - 1) {
    	val splits = svmdata.randomSplit(Array(0.6, 0.4), seed = 11L + cv_iter)
    	val training = splits(0).cache()
    	val test = splits(1)

      val tr_1 = training.filter(r => r.label == 1.0).count.toDouble / training.count
      val tr_0 = training.filter(r => r.label == 0.0).count.toDouble / training.count
      val te_1 = test.filter(r => r.label == 1).count.toDouble / test.count
      val te_0 = test.filter(r => r.label == 0).count.toDouble / test.count
      println("tr_0, tr_1, te_0, te_1 values are ", tr_0, tr_1, te_0, te_1)
      
    	val numIterations = 100
    	val model = SVMWithSGD.train(training, numIterations)

    	// Clear the default threshold.
    	model.clearThreshold()

    			// Compute raw scores on the test set. 
    	val scoreAndLabels = test.map { point =>
    			val score = model.predict(point.features)
    			(score, point.label)
    	}
    	val labelAndPreds = training.map { point =>
    	val prediction = model.predict(point.features)
    	      (point.label, prediction) 
       }
    	val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / training.count
    	println("Training Error = " + trainErr)

    	val labelAndPredsTest = test.map { point =>
    	val prediction = model.predict(point.features)
    			(point.label, prediction)
    	}
    	val testErr = labelAndPredsTest.filter(r => r._1 != r._2).count.toDouble / training.count
    	println("Testing Error = " + testErr)

    	scoreAndLabels.map{case(v,p) => println("score and labels = "  +v + ' ' + p)}.collect()
    	val metrics = new BinaryClassificationMetrics(labelAndPredsTest)
    	val temp = metrics.areaUnderROC()
      println("prinintg " + temp + " " + cv_iter)
      auROC(cv_iter) = temp
    	println("auROC value is " + auROC(cv_iter) + " " + cv_iter)
    }
    val sum = auROC.reduceLeft(_ + _)
    val mean = sum.toDouble / auROC.size
    println("Mean auROC value is " + mean)*/
    sc.stop()
  }
}



