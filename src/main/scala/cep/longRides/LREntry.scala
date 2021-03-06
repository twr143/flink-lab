package cep.longRides
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map
import Model._
import ExcersizeBase._
import cep.experiment.ExpEntry.MyEvent
import ch.qos.logback.classic.Level
import de.javakaffee.kryoserializers.jodatime.{JodaDateTimeSerializer, JodaLocalDateTimeSerializer}
import org.apache.flink.api.common.io.{FileInputFormat, FilePathFilter}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.types.Row
import org.joda.time.{DateTime, LocalDateTime}
import org.apache.flink.api.java.typeutils.runtime.kryo._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.slf4j.LoggerFactory
/**
  * Created by Ilya Volynin on 17.09.2018 at 13:56.
  */
object LREntry {
  def main(args: Array[String]) {
    val root = LoggerFactory.getLogger("org.apache.flink").asInstanceOf[ch.qos.logback.classic.Logger]
    root.setLevel(Level.WARN)
    val params = ParameterTool.fromArgs(args)
    val input = "rides.csv"
    val speed = 600 // events of 10 minutes are served in 1 second
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.registerTypeWithKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])
    // operate in Event-time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(parallelism)
    // get the taxi ride data stream, in order
    val rideData: DataStream[String] = env.readFile(
      new TextInputFormat(new Path(input)), input, FileProcessingMode.PROCESS_ONCE, 100,
             FilePathFilter.createDefaultFilter())//.assignTimestampsAndWatermarks(new PunctuatedWatermarkAssigner {
//      override def getWatermark(row: Row, timestamp: Long): Watermark = new Watermark(timestamp)
//    })
    val rides = rideData.filter(_.nonEmpty).map {
      l => TaxiRide.fromString(l)
    }
    val keyedRides = rides.keyBy(_.rideId)
//    keyedRides.print().setParallelism(1)
    // A complete taxi ride has a START event followed by an END event
    // This pattern is incomplete ...
    val completedRides: Pattern[TaxiRide, _ <: TaxiRide]  = Pattern
      .begin[TaxiRide]("pStart").where(e => e.isStart) // ...
    // We want to find rides that have NOT been completed within 120 minutes
    // This pattern matches rides that ARE completed.
    // Below we will ignore rides that match this pattern, and emit those that timeout.
    //     val pattern: PatternStream[TaxiRide] = CEP.pattern[TaxiRide](keyedRides, completedRides.within(Time.minutes(120)))
    // side output tag for rides that time out
         val timedoutTag = new OutputTag[TaxiRide]("timedout")
    // collect rides that timeout
    val timeoutFunction = (map: Map[String, Iterable[TaxiRide]], timestamp: Long, out: Collector[TaxiRide]) => {
      val rideStarted = map("START").head
      out.collect(rideStarted)
    }
    // ignore rides that complete on time
    val selectFunction = (map: Map[String, Iterable[TaxiRide]], out: Collector[TaxiRide]) => {
      println(s"select functions=${map.values.flatten}")
      out.collect(map("pStart").head)
    }
    //     val longRides = pattern.flatSelect(timedoutTag)(timeoutFunction)(selectFunction)
    val simpleStream = CEP.pattern(keyedRides, completedRides)

    //Thread.sleep(3000)
    //     printOrTest(longRides.getSideOutput(timedoutTag))
    printOrTest(simpleStream.flatSelect(selectFunction))
    env.execute("Long Taxi Rides (CEP)")
    Thread.sleep(3000)
  }

  def selectFn(pattern: Map[String, Iterable[TaxiRide]]): Iterable[TaxiRide] = {
    println(s"pattern.values.flatten=${pattern.values.flatten}")
    pattern("pStart")
  }
}
