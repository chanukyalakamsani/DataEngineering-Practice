
(year:chararray, month:chararray, maxtemp:float, mintemp:float, frost:int, rainfall:float, sunshine:float) -- method of defining schema
Source = LOAD '/data/weather' USING PigStorage('\t') AS (year:chararray, month:chararray, maxtemp:float, mintemp:float, frost:int, rainfall:float, sunshine:float);
Data = FILTER Source BY maxtemp IS NOT NULL AND mintemp IS NOT NULL;
Readings = FILTER Data BY year != 'yyyy';
YearGroups = GROUP Readings BY year;
AggTemps = FOREACH YearGroups GENERATE group AS year, AVG(Readings.maxtemp) AS avghigh, AVG(Readings.mintemp) AS avglow;
SortedResults = ORDER AggTemps BY year;
DUMP  statment is action statment in PIG
DUMP SortedResults;
quit;
