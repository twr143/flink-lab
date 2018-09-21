package cep.lR2
import java.time.{OffsetDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale
import ch.qos.logback.classic.Level
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
/**
  * Created by Ilya Volynin on 19.09.2018 at 12:11.
  */
object LR2Entry {
  val patternT = "yyyy-MM-dd HH:mm:ss"

  implicit val timeFormatter: DateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern(patternT).withLocale(Locale.US).withZone(ZoneId.systemDefault())

  implicit val typeInfoTaxiRide: TypeInformation[TaxiRide] = TypeInformation.of(classOf[TaxiRide])

  implicit def textToDate(text: String)(implicit timeFormatter: DateTimeFormatter): ZonedDateTime =
    ZonedDateTime.parse(text, timeFormatter)
  case class TaxiRide(id: Long, kind: String, time: java.time.ZonedDateTime)(implicit timeFormatter: DateTimeFormatter) {
    override def toString: String = "[" + id + " " + kind + " " + timeFormatter.format(time) + "]"
  }
  case class TaxiRideAggregated(id: Long, concatenatedValue: String)
  def main(args: Array[String]): Unit = {
    val root = LoggerFactory.getLogger("org.apache.flink").asInstanceOf[ch.qos.logback.classic.Logger]
    root.setLevel(Level.WARN)
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val eventStream = env.fromElements(
      TaxiRide(1, "S", "2018-09-17 15:00:01"), TaxiRide(1, "E", "2018-09-18 11:00:01"),
      TaxiRide(2, "S", "2018-09-18 15:10:01"), TaxiRide(2, "E", "2018-09-18 16:00:01"),
      TaxiRide(3, "S", "2018-09-18 16:53:01"), TaxiRide(3, "E", "2018-09-18 19:05:01"),
      TaxiRide(4, "S", "2018-09-18 19:53:01"), TaxiRide(4, "E", "2018-09-18 21:13:01"),
      TaxiRide(5, "S", "2018-09-18 21:47:38")
    )
    val patternCompletedRide: Pattern[TaxiRide, _ <: TaxiRide] = Pattern
      .begin[TaxiRide]("pS").where(e => e.kind == "S")
      .next("pE").where(e => e.kind == "E").where(
      (value, ctx) => {
        lazy val startTime = ctx.getEventsForPattern("pS").map(_.time).head
        ChronoUnit.MINUTES.between(startTime, value.time) < 120
      }
    )
      .within(Time.seconds(5))
    val patternNotCompleterRide: Pattern[TaxiRide, _ <: TaxiRide] = Pattern
      .begin[TaxiRide]("pS").where(e => e.kind == "S")
      .notNext("pE")
    //can't combine
    val patternNextStream: PatternStream[TaxiRide] = CEP.pattern(eventStream.keyBy(_.id)
      //
      // то есть шаблон применяется к событиям внутри одного ключа
      , patternCompletedRide)
    val resultStream: DataStream[TaxiRideAggregated] = patternNextStream.flatSelect {
      (pattern: scala.collection.Map[String, Iterable[TaxiRide]], collector: Collector[TaxiRideAggregated]) =>
        println(s"pattern= $pattern")
        val started = pattern("pS").head
        val ended = pattern("pE").head
        val timeDiff = ChronoUnit.MINUTES.between(started.time, ended.time)
        val resId = started.id
        var resValue = ""
        if (timeDiff < 120)
          resValue = s"short $timeDiff" else
          resValue = s"long $timeDiff"
        collector.collect(TaxiRideAggregated(resId, resValue))
    }
    resultStream.print().setParallelism(1)
    env.execute("LongRides 2")
  }
}
