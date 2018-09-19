package cep.experiment
import ch.qos.logback.classic.Level
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.PatternStream
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.table.api.scala.StreamTableEnvironment
import scala.collection.mutable.ListBuffer
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
    val input = "events.csv"
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val csvTableSource = CsvTableSource
      .builder
      .path("events.csv")
      .field("id", Types.INT)
      .field("kind", Types.STRING)
      .field("value", Types.STRING)
      .fieldDelimiter(",")
      //        .lineDelimiter("\n")
      //        .ignoreFirstLine
      .ignoreParseErrors
      .commentPrefix("%")
      .build
    tableEnv.registerTableSource("events", csvTableSource)
    val table = tableEnv.scan("events")
        val eventStream = env.fromCollection(ListBuffer(
            MyEvent(1, "A", "1"), MyEvent(1, "C", "1"),
            MyEvent(2, "A", "2"), MyEvent(1, "B", "1"), MyEvent(2, "C", "2"),
            MyEvent(1, "A", "3"), MyEvent(1, "D", "2"), MyEvent(1, "C", "3"),
            MyEvent(1, "B", "3"))
          )
//    val collection = ListBuffer.empty[MyEvent]
//    var eventStream = env.readFile(new TextInputFormat(new Path(input)), input).map {
//      l =>
//        val fields = l.split(",")
//        val elem = MyEvent(fields(0).toInt, fields(1), fields(2))
//        collection += elem
//        elem
//    }
//    eventStream = env.fromCollection(collection)
    // doesn't work
    //     val eventStream = tableEnv.toAppendStream[MyEvent](table).process(new ProcessFunction[MyEvent, MyEvent] {
    //           override def processElement(value: MyEvent, ctx: ProcessFunction[MyEvent, MyEvent]#Context, out: Collector[MyEvent]): Unit = {
    //             out.collect(value)
    //           }
    //         })
    eventStream.print().setParallelism(1)
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
