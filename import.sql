USE DATABASE TEST1;

CREATE OR REPLACE FILE FORMAT csv_format_1
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

CREATE OR REPLACE STAGE csv_stage_1
  FILE_FORMAT = csv_format_1;

-- dump filepath
PUT file:///home/orange/snowsql/snowflake-rowery/data/FI/*.csv @csv_stage_1 AUTO_COMPRESS=TRUE;

/**/

list @csv_stage_1;

COPY INTO mytable
  FROM @csv_stage_1/ex.csv.gz
  FILE_FORMAT = (FORMAT_NAME = csv_format_1)
  ON_ERROR = 'skip_file';

SELECT * FROM mytable;

REMOVE @csv_stage_1 PATTERN='.*.csv.gz';