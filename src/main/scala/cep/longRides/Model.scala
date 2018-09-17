package cep.longRides
import com.github.nscala_time.time.Imports._
import java.io.Serializable
import java.util.Locale
/**
  * Created by Ilya Volynin on 17.09.2018 at 13:47.
  */
object Model {
  import java.time.format.DateTimeFormatter
  import java.util.Locale
  object TaxiRide {
    private val timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC

    def fromString(line: String): TaxiRide = {
      val tokens = line.split(",")
      if (tokens.length != 11) throw new RuntimeException("Invalid record: " + line)
      val ride = new TaxiRide
      try {
        ride.rideId = tokens(0).toLong
        tokens(1) match {
          case "START" =>
            ride.isStart = true
            ride.startTime = DateTime.parse(tokens(2), timeFormatter)
            ride.endTime = DateTime.parse(tokens(3), timeFormatter)
          case "END" =>
            ride.isStart = false
            ride.endTime = DateTime.parse(tokens(3), timeFormatter)
            ride.startTime = DateTime.parse(tokens(2), timeFormatter)
          case _ =>
            throw new RuntimeException("Invalid record: " + line)
        }
        ride.startLon = if (tokens(4).length > 0) tokens(4).toFloat
        else 0.0f
        ride.startLat = if (tokens(5).length > 0) tokens(5).toFloat
        else 0.0f
        ride.endLon = if (tokens(6).length > 0) tokens(6).toFloat
        else 0.0f
        ride.endLat = if (tokens(7).length > 0) tokens(7).toFloat
        else 0.0f
        ride.passengerCnt = tokens(8).toShort
        ride.taxiId = tokens(9).toLong
        ride.driverId = tokens(10).toLong
      } catch {
        case nfe: NumberFormatException =>
          throw new RuntimeException("Invalid record: " + line, nfe)
      }
      ride
    }
  }
  class TaxiRide() extends Comparable[TaxiRide] {
    this.startTime = null

    this.endTime = null

    def this(rideId: Long, isStart: Boolean, startTime: Nothing, endTime: Nothing, startLon: Float, startLat: Float, endLon: Float, endLat: Float, passengerCnt: Short, taxiId: Long, driverId: Long) {
      this()
      this.rideId = rideId
      this.isStart = isStart
      this.startTime = startTime
      this.endTime = endTime
      this.startLon = startLon
      this.startLat = startLat
      this.endLon = endLon
      this.endLat = endLat
      this.passengerCnt = passengerCnt
      this.taxiId = taxiId
      this.driverId = driverId
    }

    var rideId = 0L

    var isStart = false

    var startTime: DateTime = _

    var endTime: DateTime = _

    var startLon = .0

    var startLat = .0

    var endLon = .0

    var endLat = .0

    var passengerCnt = 0

    var taxiId = 0L

    var driverId = 0L

    override def toString: String = {
      val sb = new StringBuilder
      sb.append(rideId).append(",")
      sb.append(if (isStart) "START"
      else "END").append(",")
      sb.append(startTime.toString(TaxiRide.timeFormatter)).append(",")
      sb.append(endTime.toString(TaxiRide.timeFormatter)).append(",")
      sb.append(startLon).append(",")
      sb.append(startLat).append(",")
      sb.append(endLon).append(",")
      sb.append(endLat).append(",")
      sb.append(passengerCnt).append(",")
      sb.append(taxiId).append(",")
      sb.append(driverId)
      sb.toString
    }

    // sort by timestamp,
    // putting START events before END events if they have the same timestamp
    override def compareTo(other: TaxiRide): Int = {
      if (other == null) return 1
      val compareTimes = this.getEventTime.compareTo(other.getEventTime)
      if (compareTimes == 0) if (this.isStart == other.isStart) 0
      else if (this.isStart) -1
      else 1
      else compareTimes
    }

    override def equals(other: Any): Boolean = other.isInstanceOf[TaxiRide] && this.rideId == other.asInstanceOf[TaxiRide].rideId

    override def hashCode: Int = this.rideId.toInt

    def getEventTime: Long = if (isStart) startTime.getMillis
    else endTime.getMillis

    //      def getEuclideanDistance(longitude: Double, latitude: Double): Double = if (this.isStart) GeoUtils.getEuclideanDistance(longitude.toFloat, latitude.toFloat, this.startLon, this.startLat)
    //      else GeoUtils.getEuclideanDistance(longitude.toFloat, latitude.toFloat, this.endLon, this.endLat)
  }


}
