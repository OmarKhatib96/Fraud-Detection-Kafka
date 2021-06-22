package org.myorg.quickstart

import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

import scala.math.max


class TimestampExtractorClicks extends AssignerWithPunctuatedWatermarks [CompteurClicks] with Serializable {
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(cliques:  CompteurClicks,prevElementTimestamp:Long) = {
    val timestamp=cliques.timestamp.toLong*1000
    currentMaxTimestamp= max(timestamp, currentMaxTimestamp)
    timestamp
  }
  override def checkAndGetNextWatermark(lastElement: CompteurClicks, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp)
  }
}