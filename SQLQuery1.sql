CREATE MASTER KEY;

CREATE DATABASE SCOPED CREDENTIAL ASBSecret 
WITH IDENTITY = 'flightdelaysclient'
,    Secret = '2fKzmw+0q9dRQJjtW/GbcL2AY9S/JWfQfX0vwqdB2l95gkp1wOIAN+8KvVrjxlNmV42QNUU/9JmwETpOB0IcgQ=='
;


CREATE EXTERNAL DATA SOURCE azure_storage 
WITH
(
    TYPE = HADOOP
,   LOCATION ='wasbs://flightdata@rsazsolawtsa.blob.core.windows.net/flights'
,   CREDENTIAL = ASBSecret
);   

CREATE EXTERNAL FILE FORMAT text_file_format 
WITH 
(   
    FORMAT_TYPE = DELIMITEDTEXT 
,   FORMAT_OPTIONS  (
                        FIELD_TERMINATOR =','
                    ,   USE_TYPE_DEFAULT = TRUE
                    )
);
CREATE EXTERNAL TABLE FlightDelays(OriginAirportCode char(3), Month tinyint, Day tinyint, Hour tinyint, DayOfWeek tinyint, Carrier varchar(4), DestAirportCode char(3), DepDelay15 bit, WindSpeed smallint, SeaLevelPressure decimal(4,2),   
HourlyPrecipitation decimal(8,5), DelayPredicted bit, DelayProbability decimal(5,4), 	OriginLatitude decimal(14,10), OriginLongitude decimal(14,10)) 
WITH
(
LOCATION = '/Scored_FlightsAndWeather.csv',
DATA_SOURCE = azure_storage,
FILE_FORMAT = text_file_format,
REJECT_TYPE = value,
REJECT_VALUE = 100000
);

SELECT Top 100 * FROM FlightDelays;

CREATE VIEW FlightDelaysSummary AS
SELECT  OriginAirportCode, Cast(OriginLatitude as varchar(15)) + ',' + Cast(OriginLongitude as varchar(15))  OriginLatLong, Month, Day, Hour, Sum(Cast(DelayPredicted as int)) NumDelays, Avg(DelayProbability) AvgDelayProbability 
FROM FlightDelays 
WHERE Month = 4
GROUP BY OriginAirportCode, Cast(OriginLatitude as varchar(15)) + ',' + Cast(OriginLongitude as varchar(15)) , Month, Day, Hour
Having Sum(Cast(DelayPredicted as int) ) > 1


SELECT * FROM FlightDelaysSummary