package org.myorg.quickstart

import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

import scala.math.max


class TimestampExtractorDisplays extends AssignerWithPunctuatedWatermarks [CompteurDisplays] with Serializable {
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(displays:  CompteurDisplays,prevElementTimestamp:Long) = {
    val timestamp=displays.timestamp.toLong*1000
    currentMaxTimestamp= max(timestamp, currentMaxTimestamp)
    timestamp
  }
  override def checkAndGetNextWatermark(lastElement: CompteurDisplays, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp)
  }
}
