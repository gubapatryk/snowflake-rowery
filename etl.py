import snowflake.connector
from datetime import datetime
import os, sys

SCHEMA = datetime.now().strftime("DUMP_%Y%m%d")
DATA_DUMP_DIR = '/home/orange/snowsql/snowflake-rowery/data/'
DATABASES = ['HR', 'FI','Sales']
dirs = os.listdir('./data')


# Gets the version
ctx = snowflake.connector.connect(
    user='alasatuk1234',
    password='Alasatuk1234',
    account='ug96869.eu-north-1.aws',
    )

for CURRENT_DATABASE in DATABASES:
    try:
        cs = ctx.cursor()
        files = os.listdir(f'./data/{CURRENT_DATABASE}')
        cs.execute(f'USE DATABASE {CURRENT_DATABASE}')
        
        cs.execute(f'CREATE OR REPLACE SCHEMA {SCHEMA}')

        from codecs import open
        with open(f'create_tables_{CURRENT_DATABASE}.sql', 'r', encoding='utf-8') as f:
            for cur in ctx.execute_stream(f):
                for ret in cur:
                    print(ret)
        cs.execute("CREATE OR REPLACE FILE FORMAT csv_format_1 TYPE = 'CSV' FIELD_DELIMITER = ';' SKIP_HEADER = 1 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE DATE_FORMAT=YYYYMMDD;")
        cs.execute("CREATE OR REPLACE FILE FORMAT csv_format_2 TYPE = 'CSV' FIELD_DELIMITER = ';' SKIP_HEADER = 1 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE DATE_FORMAT=\"YYYY-MM-DD\";")
        cs.execute("CREATE OR REPLACE FILE FORMAT csv_format_3 TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE DATE_FORMAT=YYYYMMDD;")
        cs.execute("CREATE OR REPLACE STAGE csv_stage_1;")
        cs.execute(f'PUT file://{DATA_DUMP_DIR}{CURRENT_DATABASE}/*.csv @csv_stage_1 AUTO_COMPRESS=TRUE;')
        cs.execute('list @csv_stage_1;')
        cs.execute(f'USE SCHEMA {SCHEMA}')
        for f in files:
            if (CURRENT_DATABASE == "Sales"):
                if f == "BusinessPartners.csv":
                    cs.execute(f'COPY INTO {CURRENT_DATABASE}.{SCHEMA}.{f[:-4]} FROM @CSV_STAGE_1/{f[:-4]}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_2) ON_ERROR = \'skip_file\';')
                else:
                    cs.execute(f'COPY INTO {CURRENT_DATABASE}.{SCHEMA}.{f[:-4]} FROM @CSV_STAGE_1/{f[:-4]}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_1) ON_ERROR = \'skip_file\';')
            elif (CURRENT_DATABASE == "FI"):
                if f == "FinancialTransactions.csv":
                    cs.execute(f'COPY INTO {CURRENT_DATABASE}.{SCHEMA}.{f[:-4]} FROM @CSV_STAGE_1/{f[:-4]}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_1) ON_ERROR = \'skip_file\';')
                else:
                    cs.execute(f'COPY INTO {CURRENT_DATABASE}.{SCHEMA}.{f[:-4]} FROM @CSV_STAGE_1/{f[:-4]}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_3) ON_ERROR = \'skip_file\';')
            
            elif (CURRENT_DATABASE == "HR"):
                if f == "EmployeeHeadcount.csv" or f == "EmployeePerformance.csv" or f == "EmployeePersonalData.csv" or f == "EmployeePosition.csv":
                    cs.execute(f'COPY INTO {CURRENT_DATABASE}.{SCHEMA}.{f[:-4]} FROM @CSV_STAGE_1/{f[:-4]}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_1) ON_ERROR = \'skip_file\';')
                else:
                    cs.execute(f'COPY INTO {CURRENT_DATABASE}.{SCHEMA}.{f[:-4]} FROM @CSV_STAGE_1/{f[:-4]}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_3) ON_ERROR = \'skip_file\';')
        cs.execute(f'CREATE OR REPLACE SCHEMA {CURRENT_DATABASE}.latest CLONE {CURRENT_DATABASE}.{SCHEMA};')
    except Exception as e:
        print(e)
    finally:
        cs.close()