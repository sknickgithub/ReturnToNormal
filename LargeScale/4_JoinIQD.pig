set mapreduce.reduce.memory.mb 7196

inrix_data = LOAD 'Skylar/CE650C/INRIXEvents/*' using PigStorage(',') AS (code:chararray, speed:float, time:datetime, conf:int, cvalue:int, EventID:chararray, SegOrder:int, start:chararray, end:chararray);

iqd_data = LOAD 'Skylar/CE650C/IQD/*' using PigStorage(',') AS (code2:chararray, weekday:int, hour2:int, period:int, grp_week:int, med_speed:float, iqd_speed:float);

inrix2_data = FOREACH inrix_data GENERATE code, speed, time, EventID, SegOrder, GetHour(time) as hour:int, (int) CEIL(GetMinute(time)/15) AS period:int, (int)(DaysBetween(time, ToDate('1970-01-04','yyyy-MM-dd')))%7 AS weekday:int, GetWeek(time) AS grp_week:int;

join_data = JOIN inrix2_data BY (code, hour, period, weekday, grp_week) LEFT OUTER, iqd_data BY (code2, hour2, period, weekday, grp_week);

clean_data = FOREACH join_data GENERATE EventID, code, SegOrder, speed, time, med_speed, iqd_speed, med_speed-(2*iqd_speed) AS IQDspeed, (med_speed-(2*iqd_speed) < 45 ? (med_speed-(2*iqd_speed)):45) AS Threshold, hour, GetMinute(time) as minute;

clean2_data = FOREACH clean_data GENERATE EventID, code, SegOrder, speed, time, med_speed, iqd_speed, Threshold, (speed < Threshold ? 1:0) AS Congestion, ((hour*60)+minute) AS timeminute;

STORE clean2_data INTO 'Skylar/CE650C/FinalJoin' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');

--B = LIMIT clean2_data 10;
--DUMP B;