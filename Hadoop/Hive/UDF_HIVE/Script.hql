
 -- assigning the python file in all the nodes
add file wasb:///data/convert_rain.py;

-- Trasform helps to subset the required columns in the final ouput
-- Required weather table

SELECT TRANSFORM (year, month, max_temp, min_temp, frost_days, rainfall, sunshine_hours)
USING 'python convert_rain.py' AS
(year INT, month INT, max_temp FLOAT, min_temp FLOAT, frost_days INT, rainfall FLOAT, sunshine_hours FLOAT) FROM weather;
