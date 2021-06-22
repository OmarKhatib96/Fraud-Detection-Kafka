package org.myorg.quickstart

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class ClickPerWindow extends WindowFunction[CompteurClicks, ClicksByWindow, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
                      uid: String,
                      window: TimeWindow,
                      vals: Iterable[CompteurClicks],
                      out: Collector[ClicksByWindow]): Unit = {


    // compute the counting
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (r.timestamp.toDouble.toInt, c._2 + r.compteur))
    val avgTemp = sum

    // emit a SensorReading with the average temperature
    out.collect(ClicksByWindow(uid, window.getEnd, avgTemp))
  }
}
