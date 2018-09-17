name := "flink-lab"

version := "1.0"

scalaVersion := "2.11.12"


resolvers += Resolver.jcenterRepo

val flinkVersion = "1.6.0"
libraryDependencies ++= Seq(
  //  "log4j" % "log4j" % "1.2.17",
//  "org.slf4j" % "slf4j-simple" % "1.7.25",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion,
  "org.apache.flink" %% "flink-cep-scala" % flinkVersion,
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion,
  "com.github.nscala-time" %% "nscala-time" % "2.20.0",
  "de.javakaffee" % "kryo-serializers" % "0.42",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

libraryDependencies ++= Seq(
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "0.29.22" % Compile,
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "0.29.22" % Provided // required only in compile-time
)


mainClass in(Compile, run) := Some("SocketWindowWordCount")

