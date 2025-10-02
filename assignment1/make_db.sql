DROP TABLE IF EXISTS all_taxi_info;

CREATE TABLE all_taxi_info (
    trip_id        INT,
    call_type      CHAR(1),              
    origin_call    INT NULL,             
    origin_stand   INT NULL,             
    taxi_id        INT,
    timestamp_     DATETIME,             
    day_type       CHAR(1),              
    missing_data   BOOLEAN,           
    polyline       JSON,                 
    KEY idx_taxi_id (taxi_id),
    KEY idx_timestamp (timestamp_)
);


LOAD DATA LOCAL INFILE 'cleaned_porto_data.csv'
INTO TABLE all_taxi_info
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@trip_id, @call_type, @origin_call, @origin_stand, @taxi_id, @ts, @day_type, @missing_data, @polyline)
SET
  trip_id      = NULLIF(@trip_id,''),
  call_type    = NULLIF(@call_type,''),
  origin_call  = NULLIF(@origin_call,''),
  origin_stand = NULLIF(@origin_stand,''),
  taxi_id      = NULLIF(@taxi_id,''),
  -- Hvis CSV har Unix-tid (sekunder):
  timestamp_   = IF(@ts='', NULL, FROM_UNIXTIME(@ts)),
  -- Hvis CSV har ferdig formatert dato/tid, bruk:
  -- timestamp_   = STR_TO_DATE(@ts, '%Y-%m-%d %H:%i:%s'),
  day_type     = NULLIF(@day_type,''),
  missing_data = CASE
                   WHEN LOWER(@missing_data) IN ('1','true','t','yes','y') THEN 1
                   ELSE 0
                 END,
  polyline     = IF(JSON_VALID(@polyline), CAST(@polyline AS JSON), NULL);