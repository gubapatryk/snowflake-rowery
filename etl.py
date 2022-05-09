import snowflake.connector
from datetime import datetime
import os, sys

SCHEMA = datetime.now().strftime("DUMP_%Y%m%d")
DATA_DUMP_DIR = '/home/orange/snowsql/snowflake-rowery/data/'
DATABASES = ['Sales']
dirs = os.listdir('./data')


# Gets the version
ctx = snowflake.connector.connect(
    user='kajak',
    password='HasloDoWH1!',
    account='ep27219.switzerland-north.azure',
    )

for CURRENT_DATABASE in DATABASES:
    cs = ctx.cursor()
    try:
        files = os.listdir(f'./data/{CURRENT_DATABASE}')
        cs.execute(f'USE DATABASE {CURRENT_DATABASE}')
        
        cs.execute(f'CREATE OR REPLACE SCHEMA {SCHEMA}')

        from codecs import open
        with open(f'create_tables_{CURRENT_DATABASE}.sql', 'r', encoding='utf-8') as f:
            for cur in ctx.execute_stream(f):
                for ret in cur:
                    print(ret)

        cs.execute("CREATE OR REPLACE FILE FORMAT csv_format_1 TYPE = 'CSV' FIELD_DELIMITER = ';' SKIP_HEADER = 1;")
        cs.execute("CREATE OR REPLACE STAGE csv_stage_1 FILE_FORMAT = csv_format_1;")
        print(f'PUT file://{DATA_DUMP_DIR}{CURRENT_DATABASE}/*.csv @csv_stage_1 AUTO_COMPRESS=TRUE;')
        cs.execute(f'PUT file://{DATA_DUMP_DIR}{CURRENT_DATABASE}/*.csv @csv_stage_1 AUTO_COMPRESS=TRUE;')
        for row in cs:
            print(row[0])
        cs.execute('list @csv_stage_1;')
        for row in cs:
            print(row[0])
        cs.execute(f'USE SCHEMA {SCHEMA}')
        for f in files:
            print(f)
            t = f[:-4].upper()
            print(f'COPY INTO "{t}" FROM @csv_stage_1/{t}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_1) ON_ERROR = \'skip_file\';')
            cs.execute(f'COPY INTO "{t}" FROM @csv_stage_1/{t}.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_1) ON_ERROR = \'skip_file\';')
    except Exception as e:
        print('b'+ str(e))
    finally:
        #cs.execute("REMOVE @csv_stage_1 pattern='.*.csv.gz';")
        cs.close()

ctx.close()