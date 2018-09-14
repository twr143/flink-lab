package ser_on

import java.util.{Properties, UUID}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.types.Row
import com.github.plokhotnyuk.jsoniter_scala.core._
/**
  * Created by Ilya Volynin on 14.09.2018 at 18:26.
  */
object Demo1 {
  def main(args:Array[String]){
    import Model._

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val properties = new Properties()
      properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", s"myflinkservice_${UUID.randomUUID}")
    properties.setProperty("auto.offset.reset", "earliest")
      val kafkaConsumer011 = new FlinkKafkaConsumer011[String]("testT1", new SimpleStringSchema(), properties)
      val stream: DataStream[String] = env.addSource[String](kafkaConsumer011)
      val result: DataStream[Row] = stream.filter(_.nonEmpty).flatMap{ p =>
        val obj = readFromArray(p.getBytes("UTF-8"))
        obj match {
          case o: SerializationBeanJsoniter => println("1")
          case o: SerializationBeanJsoniter2 => println("2")
          case o: SerializationBeanJsoniter3 => println("3")
          case _ => println("illegal obj")
        }
        Seq(Row.of(obj))
      }
      result.print().setParallelism(1)
      env.execute()
    }}
