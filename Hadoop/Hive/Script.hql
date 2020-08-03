-- creating a table

CREATE TABLE rawlog (log_date STRING, log_time STRING, c_ip STRING, cs_username STRING, s_ip STRING,
					 s_port STRING, cs_method STRING, cs_uri_stem STRING, cs_uri_query STRING, sc_status STRING,
					 sc_bytes INT, cs_bytes INT, time_taken INT, cs_user_agent STRING, cs_referrer STRING)
					  ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ';

-- Loding data into tables

LOAD DATA INPATH '/data/input' INTO TABLE rawlog;

-- External tables


CREATE EXTERNAL TABLE cleanlog
		(log_date DATE, log_time STRING, c_ip STRING, cs_username STRING, s_ip STRING,
 		s_port STRING, cs_method STRING, cs_uri_stem STRING, cs_uri_query STRING,
 		sc_status STRING, sc_bytes INT, cs_bytes INT, time_taken INT, cs_user_agent STRING, cs_referrer STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
STORED AS TEXTFILE LOCATION '/data/cleanlog';


-- Inserting from one table to another


INSERT INTO TABLE cleanlog
SELECT *
FROM rawlog
WHERE SUBSTR(log_date, 1, 1) <> '#';


--  partitions

CREATE EXTERNAL TABLE partitionedlog
(log_day int, log_time STRING, c_ip STRING, cs_username STRING, s_ip STRING, s_port STRING, cs_method STRING, cs_uri_stem STRING, cs_uri_query STRING, sc_status STRING, sc_bytes INT, cs_bytes INT, time_taken INT, cs_user_agent STRING, cs_referrer STRING)
PARTITIONED BY (log_year int, log_month int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
STORED AS TEXTFILE LOCATION '/data/partitionedlog';


-- selection from the partitioned tables

SELECT log_day, count(*) AS page_hits
FROM partitionedlog
WHERE log_year=2008 AND log_month=6
GROUP BY log_day;
