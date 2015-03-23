name := "MyLogging"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.2.1",
"org.apache.spark" %% "spark-mllib" % "1.2.1"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
