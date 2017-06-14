set mapreduce.reduce.memory.mb 7196

inrix_data = LOAD 'Skylar/CE650C/SampleOutput' using PigStorage(',') AS (code:chararray, cvalue:int, SegmentClosed:int, conf:int, speed:float, average:float, reference:float, travel:float, time:datetime);

list_data = LOAD 'Skylar/CE650C/SmallList/SmallSelectedXDSegments.csv' using PigStorage(',') AS (xd:chararray);

join_data = JOIN inrix_data BY code, list_data BY xd;

--STORE final_data INTO 'Skylar/CE650C/SampleOutput3' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');

B = LIMIT join_data 10;
DUMP B;