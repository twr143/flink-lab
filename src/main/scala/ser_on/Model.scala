package ser_on
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

/**
  * Created by Ilya Volynin on 14.09.2018 at 18:29.
  */
object Model {
  sealed trait SerializationBeanJsoniterBase
  case class SerializationBeanJsoniter(first: String, second: Int, third: java.time.OffsetDateTime) extends SerializationBeanJsoniterBase
  case class SerializationBeanJsoniter2(first: String, second: Int, third: java.time.OffsetDateTime, fourth: Int) extends SerializationBeanJsoniterBase
  case class SerializationBeanJsoniter3(first: Int) extends SerializationBeanJsoniterBase
  implicit val codec: JsonValueCodec[SerializationBeanJsoniterBase] = JsonCodecMaker.make[SerializationBeanJsoniterBase](CodecMakerConfig())

}
