package streaming

import java.sql.Timestamp

sealed trait VehicleUpdate

case class VehicleLocation(vehicle_id: String, lat_long:String, elevation:String, speed: Double, acceleration: Double, time_period:Timestamp, collect_time:Timestamp, tile2: String, fuel_level:Double =0, mileage:Double =0, solr_query:String = "") extends VehicleUpdate{
  override def toString: String = {
    //when acceleration gets parse
    val speed_str:String = if (speed == 0.0) "0" else speed.toString
    val acc_str = if (acceleration == 0.0)  "0" else acceleration.toString
    val vl_str = s"$vehicle_id,$lat_long,$elevation,$speed_str,$acc_str,${time_period.getTime},${collect_time.getTime},$tile2"
    println(s"vl_str $vl_str")
    vl_str
  }
}

case class VehicleEvent(vehicle_id:String, event_name:String, event_value:String, time_period:Timestamp, collect_time:Timestamp, solr_query:String = "") extends VehicleUpdate {
  override def toString:String = {
    s"$vehicle_id,$event_name,$event_value,${time_period.getTime},${collect_time.getTime}"
  }
}