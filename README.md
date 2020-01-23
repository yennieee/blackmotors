# blackmotors

package com.hkmc.bigdata.vcrm.datainteg.main

import com.hkmc.bigdata.vcrm.datainteg.main.conf.{ConfigSubmit, ConfigValues}
import com.hkmc.bigdata.vcrm.datainteg.processor.BaseProcessor
import com.hkmc.bigdata.vcrm.datainteg.processor.summary.DummyProcessor
import com.hkmc.bigdata.vcrm.datainteg.repeat.{DummyRepeater, Repeater}
import com.hkmc.bigdata.vcrm.datainteg.sink.Sink
import com.hkmc.bigdata.vcrm.datainteg.source.SourceBase
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * MainApp Object<br>
  * Config 파일을 읽어 Spark Submit을 실행한다.
  */
object MainApp extends App {
  private val configs = ConfigValues.loadConfig(args)
  private val sparkConf = ConfigValues.getSparkConf(configs)
  private val configsList = ConfigValues.getConfigList(configs, "configs")

  private val context = new HiveContext(new SparkContext(sparkConf))

  try {
    configsList.foreach(conf => {
      val repeatConfig = ConfigValues.getConfig(conf, "repeat")
      val processorConfigs = ConfigValues.getConfigs(conf, "processor")

      val repeater = if (repeatConfig == None) new DummyRepeater() else Repeater(repeatConfig.get)
      val sources = ConfigValues.getConfigs(conf, "source").map(SourceBase(_)).toMap
      val processors = if (processorConfigs.isEmpty) Seq(new DummyProcessor()) else processorConfigs.map(BaseProcessor(_))
      val sinks = ConfigValues.getConfigs(conf, "sink").map(Sink(_))

      new ConfigSubmit(repeater, sources, processors, sinks).repeat(context)
    })
  } finally {
    context.sparkContext.stop()
  }
}
