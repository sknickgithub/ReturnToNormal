set mapreduce.reduce.memory.mb 7196

inrix_data = LOAD 'Skylar/CE650C/INRIXAllData/*' using PigStorage(',') AS (code:chararray, cvalue:int, SegmentClosed:int, conf:int, speed:float, average:float, reference:float, travel:float, time:datetime, xd2:chararray);

event_data = LOAD 'Skylar/CE650C/Events/Events.csv' using PigStorage(',') AS (xd:chararray, EventID:chararray, SegOrder:int, Date:datetime, EventID2:chararray, Start:chararray, End:chararray, StartMonth:int, StartDay:int, StartHour:int, EndMonth:int, EndDay:int, EndHour:int);

join_data = JOIN inrix_data BY (code, GetMonth(time), GetDay(time)), event_data BY (xd, StartMonth, StartDay);

clean_data = FOREACH join_data GENERATE code, speed, time, conf, cvalue, EventID, SegOrder, StartMonth, StartDay, StartHour, EndMonth, EndDay, EndHour, Start, End;

filter_data = FILTER clean_data BY GetMonth(time)==StartMonth AND GetDay(time)==StartDay AND GetHour(time)>=StartHour AND GetHour(time)<=EndHour;

final_data = FOREACH filter_data GENERATE code, speed, time, conf, cvalue, EventID, SegOrder, Start, End;

STORE final_data INTO 'Skylar/CE650C/INRIXEvents' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');

--B = LIMIT final_data 10;
--DUMP B;