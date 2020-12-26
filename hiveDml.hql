DROP TABLE COVID_OUTPUT;
CREATE EXTERNAL TABLE COVID_OUTPUT(state varchar(50),  Cured varchar(20), Deaths varchar(20), Confirmed varchar(20) ) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION 'wasbs://outputhive@staginm13.blob.core.windows.net/outputhive';
INSERT OVERWRITE TABLE COVID_OUTPUT select state,sum(cast (cured as int)),sum(cast (deaths as int)),sum(cast (confirmed  as int) ) from covid_cases group by state;
