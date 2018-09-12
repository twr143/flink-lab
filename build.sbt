name := "flink-lab"

version := "1.0"

scalaVersion := "2.11.12"


resolvers += Resolver.jcenterRepo

libraryDependencies ++= Seq(
//  "log4j" % "log4j" % "1.2.17",
//  "org.slf4j" % "slf4j-simple" % "1.7.25",
"org.apache.flink" %% "flink-streaming-scala" % "1.6.0"
)

mainClass in (Compile, run) := Some("SocketWindowWordCount")

