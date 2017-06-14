set mapreduce.reduce.memory.mb 7196

inrix_data = LOAD 'Skylar/CE650C/FinalJoin/*' using PigStorage(',') AS (EventID:chararray, code:chararray, SegOrder:int, speed:int, time:datetime, med_speed:int, iqd_speed:int, Threshold:int, Congestion:int, timeminute:int);

list_data = LOAD 'Skylar/CE650C/EventSubset/18.csv' using PigStorage(',') AS (EventID2:chararray);

join_data = JOIN inrix_data BY EventID, list_data BY EventID2 using 'REPLICATED';

final_data = FOREACH join_data GENERATE EventID, code, SegOrder, speed, time, med_speed, iqd_speed, Threshold, Congestion, timeminute;

STORE final_data INTO 'Skylar/CE650C/Subset18' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');

--B = LIMIT join_data 11;
--DUMP B;