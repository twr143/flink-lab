package cep.longRides
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
/**
  * Created by Ilya Volynin on 17.09.2018 at 14:28.
  */
object ExcersizeBase {
  import cep.longRides.Model.TaxiRide
    import org.apache.flink.streaming.api.functions.source.SourceFunction
    var rides: SourceFunction[Model.TaxiRide] = _

    var fares: SourceFunction[Nothing] = _

    var strings: SourceFunction[String] = _

    var out: SinkFunction[_] = _

    var parallelism = 4

    val pathToRideData = "/Users/david/stuff/flink-training/trainingData/nycTaxiRides.gz"

    val pathToFareData = "/Users/david/stuff/flink-training/trainingData/nycTaxiFares.gz"

    def rideSourceOrTest(source: SourceFunction[Model.TaxiRide]): SourceFunction[Model.TaxiRide] = {
      if (rides == null) return source
      rides
    }

    def fareSourceOrTest(source: SourceFunction[Nothing]): SourceFunction[Nothing] = {
      if (fares == null) return source
      fares
    }

    def stringSourceOrTest(source: SourceFunction[String]): SourceFunction[String] = {
      if (strings == null) return source
      strings
    }

    def printOrTest(ds: DataStream[_]): Unit = {
      println("printing the result...")
      ds.print.setParallelism(1)
    }

}
