package ser_on
import java.util.{Properties, UUID}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.types.Row
import com.github.plokhotnyuk.jsoniter_scala.core._
/**
  * Created by Ilya Volynin on 14.09.2018 at 18:26.
  */
object Demo1 {
  def main(args: Array[String]) {
    import Model._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", s"myflinkservice_${UUID.randomUUID}")
    properties.setProperty("auto.offset.reset", "earliest")
    val kafkaConsumer011 = new FlinkKafkaConsumer011[String]("testT1", new SimpleStringSchema(), properties)
    val stream: DataStream[String] = env.addSource[String](kafkaConsumer011)
    val result: DataStream[Row] = stream.filter(_.nonEmpty).flatMap { p =>
      Seq(Row.of(readFromArray(p.getBytes("UTF-8"))))
    }
    result.map(_.getField(0) match {
      case sb: SerializationBeanJsoniter => sb.first
      case sb2: SerializationBeanJsoniter => sb2.first
      case sb3: SerializationBeanJsoniter => sb3.first
      case _ => throw new Exception("illegal object parsed")
    }).print().setParallelism(1)
    env.execute()
  }
}
