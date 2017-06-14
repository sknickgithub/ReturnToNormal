set mapreduce.reduce.memory.mb 7196
define Quantile datafu.pig.stats.StreamingQuantile('0.25', '0.50', '0.75');
inrix_data = LOAD 'Skylar/CE650C/SampleOutput/*' using PigStorage(',') AS (code:chararray, speed:float, time:datetime, conf:int, cvalue:int);

real_Data_in = FILTER inrix_data BY speed > 0 AND cvalue >= 30 AND conf == 30;
week0_in = FOREACH real_Data_in GENERATE code, speed, GetHour(time) AS hour:int, CEIL(GetMinute(time)/15) AS (period:int), (DaysBetween(time, ToDate('1970-01-04','yyyy-MM-dd')))%7 AS (weekday), GetWeek(time) AS week_org:int, GetWeek(time) AS grp_week:int, time;

week1_in = FOREACH week0_in GENERATE code, speed, hour, period, weekday, week_org, (grp_week + 1) AS grp_week, time;
week2_in = FOREACH week0_in GENERATE code, speed, hour, period, weekday, week_org, (grp_week + 2) AS grp_week, time;
week3_in = FOREACH week0_in GENERATE code, speed, hour, period, weekday, week_org, (grp_week + 3) AS grp_week, time;
week4_in = FOREACH week0_in GENERATE code, speed, hour, period, weekday, week_org, (grp_week + 4) AS grp_week, time;
week5_in = FOREACH week0_in GENERATE code, speed, hour, period, weekday, week_org, (grp_week + 5) AS grp_week, time;
week6_in = FOREACH week0_in GENERATE code, speed, hour, period, weekday, week_org, (grp_week + 6) AS grp_week, time;
week7_in = FOREACH week0_in GENERATE code, speed, hour, period, weekday, week_org, (grp_week + 7) AS grp_week, time;
week_all_in = UNION week0_in,week1_in,week2_in,week3_in,week4_in,week5_in,week6_in,week7_in;

group_week_in = GROUP week_all_in BY (code, weekday, hour, period, grp_week);
agg_week_in = FOREACH group_week_in GENERATE group.code AS code, group.weekday AS weekday, group.hour AS hour, group.period AS period, group.grp_week AS grp_week,  Quantile(week_all_in.speed) AS quant_speed;
params_in = FOREACH agg_week_in GENERATE code, weekday, hour, period, grp_week, quant_speed.quantile_0_5 AS med_speed, (quant_speed.quantile_0_75 - quant_speed.quantile_0_25)  AS iqd_speed;

STORE params_in INTO 'Skylar/CE650C/SampleIQD' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');

--B = LIMIT params_in 10;
--DUMP B;