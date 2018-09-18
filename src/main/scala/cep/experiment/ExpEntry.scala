package cep.experiment
import ch.qos.logback.classic.Level
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.PatternStream
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag
/**
  * Created by Ilya Volynin on 18.09.2018 at 12:51.
  */
object ExpEntry {
  case class MyEvent(id: Int, kind: String, value: String)
  case class MyAggregatedEvent(id: Int, concatenatedValue: String)
  def main(args: Array[String]) {
    val root = LoggerFactory.getLogger("org.apache.flink").asInstanceOf[ch.qos.logback.classic.Logger]
    root.setLevel(Level.WARN)
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val eventStream = env.fromElements(
      MyEvent(1, "A", "1"), MyEvent(1, "C", "1"),
      MyEvent(2, "A", "2"), MyEvent(1, "B", "1"), MyEvent(2, "C", "2"),
      MyEvent(1, "A", "3"), MyEvent(1, "D", "2"), MyEvent(1, "C", "3"),
      MyEvent(1, "B", "3")
    )
    val pattern: Pattern[MyEvent, _ <: MyEvent] = Pattern
      .begin[MyEvent]("pA").where(e => e.kind == "A")
      .followedBy("pC").where(e => e.kind == "C")
      .within(Time.seconds(5))
    val patternNextStream: PatternStream[MyEvent] = CEP.pattern(eventStream.keyBy(_.id)
      //
      // то есть шаблон применяется к событиям внутри одного ключа
      , pattern)
    val outNextStream: DataStream[MyAggregatedEvent] = patternNextStream.flatSelect {
      (pattern: scala.collection.Map[String, Iterable[MyEvent]], collector: Collector[MyAggregatedEvent]) =>
        println(s"pattern= $pattern")
        val partA = pattern("pA")
        val partC = pattern("pC")
        val resValue = partA.map(_.value).mkString(" ") + " => " + partC.map(_.value).mkString(" ")
        val resId = 1
        collector.collect(MyAggregatedEvent(resId, resValue))
    }
    outNextStream.print().setParallelism(1)
    env.execute("Experiment")
  }
}
