# blackmotors

/*
spark-shell --conf spark.ui.enabled=false --master yarn --num-executors 30 --executor-cores 4 --executor-memory 16G --driver-cores 2 --driver-memory 10G --name 6901188_SparkShell --jars  customer_segmentation-assembly-0.415.jar
*/

//package com.hkmc.bigdata.vcrm.datainteg.main

import com.hkmc.bigdata.vcrm.commons.spark.modules.tms.processors.parser.v2.DrvLogProcessorV2
import com.hkmc.bigdata.vcrm.commons.spark.modules.vindecode.processors.VinDecodeProcessor
import com.hkmc.bigdata.vcrm.commons.spark.types.sources.SourceSwift
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame // ★ 추가한 것!
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

val startDate = "20200101" // 시작날짜
  val endDate = startDate // 조건: startDate <= 파싱날짜 <= endDate
  val carList = Seq("TM") //차종
  val fuelList = Seq("G","D","null") // 연료타입
//  val savePath = "vcrm_7000066.test_tm_20200121_t" // 하이브에 저장할 테이블 이름 (외주분석계)
//  val savePath = "vcrm_7000066.test_tm_20200122_t" // 하이브에 저장할 테이블 이름 (외주분석계)
  val savePath = "vcrm_7000066.test_tm_20200129_t" // 하이브에 저장할 테이블 이름 (통합분석계)
  val tipLengthThread = 8 // 최소 시그널 길이
  val signals = { //파싱할 시그널들
    Seq("EMS11.VS", "SAS11_FS.SAS_Angle")}

  /**
    * 시그널맵을 가지고 스키마를 만들어줌
    * @param signals t, lat, lon을 포함한 시그널 리스트
    * @return StructType
    */
  def makeTableSchema(signals:Seq[String]):StructType = {
    val fields = signals.map(signal => StructField(signal,FloatType)).toArray
    return new StructType(Array(
      StructField("vin", StringType),
      StructField("ignitiontime", TimestampType),
      StructField("prj_vehl_cd", StringType),
      StructField("fuel_type", StringType)
    ) ++ fields)
  }

  val hc = new HiveContext(sc)

  //SourceSwift에서 파일 다운
  val src: SourceSwift = {
    new SourceSwift("http://10.12.55.31:35357/v3", "8IfL7WQc/6dQXNBNHp/b5w==",
      "lQ9RLmorfhwzlDAw151jNQ==", "hmc_vcrm", swiftVersion = "3.0")
      .setNumPartitions(400)
      .setContainer("vcrm-prod-svc-50-raw-archv")
      .setSearchDate(startDate,endDate)
  }

  val vinProc = {
    new VinDecodeProcessor(hc, "/user/hive/warehouse/hkmg_tms.db")
      .setVinColumnName("vin")
      .setDecodingColumns(Seq("cars", "fuel")) // 가져올 차량 정보 설정
  }
  val specPath = {Seq("/user/vcrm_assets/tms/v2_enc/vcrm2_r23_normal_2.63.07_2.70.03_20190621.canspec", // 분석계
    "/user/vcrm_assets/tms/v2_enc/vcrm2_normal_ext001_emsv_diesel_20180608.canspec.ext")}

  // VCRM 2.0 파싱
  val drvProc = {
    new DrvLogProcessorV2(hc, specPath, signals)
  }

  val carFilterd = vinProc.processDS(src.toDS(hc)).filter(row=> {
    val car = row(7)
    val fuel ={
      if (Option(row(8)).isEmpty) "null"
      else row(8)
    }
    carList.indexOf(car) != -1 && fuelList.indexOf(fuel) != -1
  }) // vin 디코딩

  //** carFilterd 확인하기**//
  //carFilterd.show(5)
  //** carFilterd Full data 보기**//
  //carFilterd.show(100, false)
  val parsed = drvProc.processDF(carFilterd) // 신호 파싱

//  parsed.sort($"vin"왜 parsed에다가 where절을 넣어야하는지?

  //test!!! 2020.01.29
  //peopleDf.where($"age" > 15)
  parsed.select("vin").show(1)
  parsed.select("ignitiontime").show(1)
  parsed.where("ignitiontime").show(1)
  parsed.filter(col("ignitiontime")<"2020-01-01 16:00").show(1)
  parsed.filter(col("ignitiontime")>"2020-01-01 14:00").show(3)

  val parsed2 = parsed.filter(col("ignitiontime")>"2020-01-01 13:00")
  val parsed3 = parsed2.filter(col("ignitiontime")<"2020-01-01 15:00")

  //parsed.where(parsed.vin==" " ).collect()  //parsed.where()

  // 스키마 생성
  val schema = makeTableSchema(Seq("t") ++ drvProc.getSignals)

//  val filterRdd = parsed.rdd.filter(row => {
    val filterRdd = parsed3.rdd.filter(row => {
    try {
      // 시그로그 길이가 tipLengthThread 보다 작으면 날리기
      val sigLog = row.getAs[Seq[Seq[Float]]]("sigLog")
      if (sigLog.length < tipLengthThread)
        throw new Exception

      // 이상한 위경도 찍혔으면 트립 날리기
      val noramlLen = sigLog.length
      val sig_len = sigLog.count(rows => (rows(0) != 37.464751 && rows(1) != 127.042975)
        && (rows(0) != 0.0 && rows(1) != 0.0))
      if (sig_len != noramlLen)
        throw new Exception

      // cause && tripLength 조건
      val cause = row.getAs[String]("cause")
      val tripLength = row.getAs[Int]("tripLength")
      if (cause != "OK" && tripLength <= 1) // tripLength가 2 이상은 돼야함
        throw new Exception

      // 시그널 안에 널 들어있는 지 체크
      val sigMap = (2 until drvProc.getSignals.length).toList.map( i => { // 0과 1은 위경도
        noramlLen != sigLog.map(sig => sig(i)).map(x => Option(x).isDefined).count(x => x)
      })
      if (sigMap.count(x=>x)!= 0)
        throw new Exception

      // 끝까지 왔으면 true 중간에 걸리면 false
      true
    } catch {
      case e: Exception => false
    }
  })

  val row_rdd = filterRdd.flatMap(row => {
    // val sigLog = row.getAs[Seq[Seq[Double]]]("sigLog")
    val sigLog = row.getAs[Seq[Seq[Float]]]("sigLog")
    val vin = row.getAs[String]("vin")
    val ignOnTime = row.getAs[java.sql.Timestamp]("ignitiontime")
    val cars = row.getAs[String]("prj_vehl_cd")
    val fuel = row.getAs[String]("fuel")

    // t 생성
    val logs = sigLog.zipWithIndex.map({
      case (frame, idx) => new GenericRowWithSchema(
//      (Seq(vin) ++ Seq(ignOnTime) ++ Seq(cars) ++ Seq(fuel) ++ Seq(idx.toDouble) ++ frame).toArray,
        (Seq(vin) ++ Seq(ignOnTime) ++ Seq(cars) ++ Seq(fuel) ++ Seq(idx.toFloat) ++ frame).toArray,
        schema).asInstanceOf[Row]
    })
    logs
  })

//** row_rdd 찍어보기 **//
//row_rdd.take(10).foreach(println)

// data 프레임으로
  val row_df = {
    hc.createDataFrame(row_rdd, schema)
  }

row_df.write.format("parquet").mode("overwrite").saveAsTable(savePath)
