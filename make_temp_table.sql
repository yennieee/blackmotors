use ${hivevar:db_name};


--${hivevar:start_date} 시작일
--${hivevar:end_date} 종료일

---------------------------------------------------------------------
------------------- 직전/직후 TRIP 구분
---------------------------------------------------------------------
DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp1;
CREATE TABLE ev_vcrm2_od_mstr_temp1 STORED AS PARQUET AS
SELECT *,
       LAG(last_t) OVER (PARTITION BY vin
                         ORDER BY ignitiontime ASC) AS pr_last_t,
       LEAD(first_t) OVER (PARTITION BY vin
                           ORDER BY ignitiontime ASC) AS nx_first_t,
       LEAD(first_soc_pc) OVER (PARTITION BY vin
                                ORDER BY ignitiontime ASC) AS nx_first_soc_pc,
       (energy_usage - ptcpwrcon_wh - acncomppwrcon_wh - pwrmon_wh) AS motor_wh,
       (ptcpwrcon_wh + acncomppwrcon_wh) AS ac_wh,
       ((energy_usage - ptcpwrcon_wh - acncomppwrcon_wh - pwrmon_wh)/period * 3600 / 1000)AS motor_kw,
       ((ptcpwrcon_wh + acncomppwrcon_wh)/period * 3600 / 1000) AS ac_kw,
       pwrmon_wh/period * 3600 / 1000 AS pwrmon_kw,
       ptcpwrcon_wh/period * 3600 / 1000 AS ptcpwrcon_kw,
       acncomppwrcon_wh/period * 3600 / 1000 AS acncomppwrcon_kw,
       1000*(distance_km/(energy_usage - ptcpwrcon_wh - acncomppwrcon_wh - pwrmon_wh)) AS kpkwh
FROM
  (SELECT vin,
  		  prj_vehl_cd,
          ignitiontime,
          if((last_soc_pc - first_soc_pc)>=5, 1, 0) AS charge,
          0 AS missing,
          first_t,
          from_unixtime(unix_timestamp(ignitiontime, 'yyyyMMddHHmmss') + period) AS last_t,
          period,
          SQRT(((last_lat-first_lat)*(last_lat-first_lat)) + ((last_long-first_long)*(last_long-first_long))) AS distance_km_sqrt,
          distance_km,
          first_soc_pc,
          last_soc_pc,
          (last_soc_pc - first_soc_pc) AS soc_amount,
          first_lat,
          first_long,
          last_lat,
          last_long,
          avg_vs,
          ptcpwrcon_wh,
          acncomppwrcon_wh,
          pwrmon_wh,
          first_energymap_1000ms,
          last_energymap_1000ms,
          (first_energymap_1000ms - last_energymap_1000ms) AS energy_usage
          ,vcrm_v
   FROM drv_rt_log_ev_from2_to1_fl
   WHERE ignitiontime between '${hivevar:start_date}' and '${hivevar:end_date}' ) A ;



---------------------------------------------------------------------
------------------- TRIP 정의를 만족하는 ROW만 추출
---------------------------------------------------------------------
DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp2;
CREATE TABLE ev_vcrm2_od_mstr_temp2 STORED AS PARQUET AS
SELECT *
FROM ev_vcrm2_od_mstr_temp1
WHERE first_soc_pc != last_soc_pc 
    OR distance_km > 0.001 ;



---------------------------------------------------------------------
------------------- 주행시간 이상 트립 제거 #1
---------------------------------------------------------------------
DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp3;
CREATE TABLE ev_vcrm2_od_mstr_temp3 STORED AS PARQUET AS
SELECT *
FROM ev_vcrm2_od_mstr_temp2
WHERE first_t >= pr_last_t
  OR last_t >= pr_last_t;



---------------------------------------------------------------------
------------------- 주행시간 이상 트립 제거 #2
---------------------------------------------------------------------
DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp4;
CREATE TABLE ev_vcrm2_od_mstr_temp4 STORED AS PARQUET AS
SELECT *
FROM ev_vcrm2_od_mstr_temp3
WHERE nx_first_t >= last_t;


---------------------------------------------------------------------
------------------- Missing Trip 추출
---------------------------------------------------------------------
DROP TABLE IF EXISTS ev_vcrm2_od_mstr_missing_temp1;
CREATE TABLE ev_vcrm2_od_mstr_missing_temp1 STORED AS PARQUET AS
SELECT vin,
	   prj_vehl_cd,
       last_t AS ignitiontime,
       if((nx_first_soc_pc - last_soc_pc)>=5, 1, 0) AS charge,
       1 AS missing,
       last_t AS first_t,
       nx_first_t AS last_t,
       period,
       0 AS distance_km,
       SQRT(((nx_first_lat-last_lat)*(nx_first_lat-last_lat)) + ((nx_first_long-last_long)*(nx_first_long-last_long))) AS distance_km_sqrt,
       last_soc_pc AS first_soc_pc,
       nx_first_soc_pc AS last_soc_pc,
       (nx_first_soc_pc - last_soc_pc) AS soc_amount,
       last_lat AS first_lat,
       last_long AS first_long,
       nx_first_lat AS last_lat,
       nx_first_long AS last_long
       ,vcrm_v
FROM
  (SELECT vin ,
  	      prj_vehl_cd,
          ignitiontime ,
          charge ,
          missing ,
          first_t ,
          last_t ,
          period ,
          distance_km,
          distance_km_sqrt ,
          first_soc_pc ,
          last_soc_pc ,
          soc_amount ,
          first_lat ,
          first_long ,
          last_lat ,
          last_long ,
          LEAD(first_t) OVER (PARTITION BY vin
                              ORDER BY ignitiontime ASC) AS nx_first_t,
          LEAD(first_soc_pc) OVER (PARTITION BY vin
                                   ORDER BY ignitiontime ASC) AS nx_first_soc_pc,
          LEAD(first_lat) OVER (PARTITION BY vin
                                ORDER BY ignitiontime ASC) AS nx_first_lat,
          LEAD(first_long) OVER (PARTITION BY vin
                                 ORDER BY ignitiontime ASC) AS nx_first_long
          ,vcrm_v
   FROM ev_vcrm2_od_mstr_temp4) A
WHERE abs(nx_first_soc_pc - last_soc_pc) > 1;


---------------------------------------------------------------------
------------------- 충전 타입 판정
---------------------------------------------------------------------
DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp5;
CREATE TABLE ev_vcrm2_od_mstr_temp5 STORED AS PARQUET AS
SELECT * ,
       CASE
           WHEN charge = 1
                AND prj_vehl_cd = 'OS'
                AND soc_amount/(period/60) < (9.43369078168267 + 3.27144625059167)/2 THEN 2
           WHEN charge = 1
                AND prj_vehl_cd = 'OS'
                AND soc_amount/(period/60) >= (9.43369078168267 + 3.27144625059167)/2
                AND soc_amount/(period/60) < (9.43369078168267+ 35.2857459526887)/2 THEN 1
           WHEN charge = 1
                AND prj_vehl_cd = 'OS'
                AND soc_amount/(period/60) >= (9.43369078168267 + 35.2857459526887)/2 THEN 0
           WHEN charge = 1
                AND prj_vehl_cd = 'DE'
                AND soc_amount/(period/60) < (2.74223926244173 + 9.04728040539945)/2 THEN 2
           WHEN charge = 1
                AND prj_vehl_cd = 'DE'
                AND soc_amount/(period/60) >= (2.74223926244173 + 9.04728040539945)/2
                AND soc_amount/(period/60) < (9.04728040539945 + 37.2108003196726)/2 THEN 1
           WHEN charge = 1
                AND prj_vehl_cd = 'DE'
                AND soc_amount/(period/60) >= (9.04728040539945 + 37.2108003196726)/2 THEN 0
           WHEN charge = 1
                AND prj_vehl_cd = 'SK3'
                AND soc_amount/(period/60) < (2.43967031107782 + 16.9615708069128)/2 THEN 2
           WHEN charge = 1
                AND prj_vehl_cd = 'SK3'
                AND soc_amount/(period/60) >= (2.43967031107782 + 16.9615708069128)/2
                AND soc_amount/(period/60) < (16.9615708069128 + 43.7598306548224)/2 THEN 1
           WHEN charge = 1
                AND prj_vehl_cd = 'SK3'
                AND soc_amount/(period/60) >= (16.9615708069128 + 43.7598306548224)/2 THEN 0
           ELSE NULL
       END AS charge_type_slope
FROM
  (SELECT vin ,
  		  prj_vehl_cd ,
          ignitiontime ,
          charge ,
          missing ,
          first_t ,
          last_t ,
          period/60 AS period ,
          distance_km,
          distance_km_sqrt ,
          first_soc_pc ,
          last_soc_pc ,
          soc_amount ,
          first_lat ,
          first_long ,
          last_lat ,
          last_long
          ,vcrm_v
   FROM ev_vcrm2_od_mstr_temp4
   UNION ALL SELECT vin ,
   					prj_vehl_cd ,
                    ignitiontime ,
                    charge ,
                    missing ,
                    first_t ,
                    last_t ,
                    period/60 AS period ,
                    distance_km,
                    distance_km_sqrt ,
                    first_soc_pc ,
                    last_soc_pc ,
                    soc_amount ,
                    first_lat ,
                    first_long ,
                    last_lat ,
                    last_long
                    ,vcrm_v
   FROM ev_vcrm2_od_mstr_missing_temp1) A ;

--중간 테이블 제거
--DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp1;
--DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp2;
--DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp3;
--DROP TABLE IF EXISTS ev_vcrm2_od_mstr_temp4;
--DROP TABLE IF EXISTS ev_vcrm2_od_mstr_missing_temp1;
