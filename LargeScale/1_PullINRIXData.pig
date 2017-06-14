set mapreduce.reduce.memory.mb 7196

inrix_data = LOAD 'InrixSegments/2016/{10,11,12}' using PigStorage(',') AS (code:chararray, cvalue:int, SegmentClosed:int, conf:int, speed:float, average:float, reference:float, travel:float, time:datetime);

list_data = LOAD 'Skylar/CE650C/XDList/FullXDList.csv' using PigStorage(',') AS (xd:chararray);

join_data = JOIN inrix_data BY code, list_data BY xd using 'REPLICATED';

STORE join_data INTO 'Skylar/CE650C/INRIXAllData' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');

--B = LIMIT join_data 10;
--DUMP B;