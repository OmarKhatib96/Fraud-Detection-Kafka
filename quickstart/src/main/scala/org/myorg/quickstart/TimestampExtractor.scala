package org.myorg.quickstart

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

import scala.math.max


class TimestampExtractorClicks extends AssignerWithPeriodicWatermarks[CompteurClicks]  {
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(clicks: CompteurClicks,prevElementTimestamp:Long) = {
    val timestamp=clicks.timestamp.toLong*1000
    currentMaxTimestamp= max(timestamp, currentMaxTimestamp)
    timestamp


  }
  override def getCurrentWatermark(): Watermark = {
    new Watermark(currentMaxTimestamp)
  }
}