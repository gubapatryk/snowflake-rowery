USE DATABASE TEST1;

CREATE TABLE mytable(
   CUSTOMERTYPEID VARCHAR(2) NOT NULL PRIMARY KEY
  ,LANGUAGE       VARCHAR(2) NOT NULL
  ,SHORT_DESCR    VARCHAR(2) NOT NULL
  ,MEDIUM_DESCR   VARCHAR(10) NOT NULL
  ,LONG_DESCR     VARCHAR(30)
);