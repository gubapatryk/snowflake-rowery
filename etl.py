import snowflake.connector
from datetime import datetime
import os, sys

SCHEMA_DATE = datetime.now().strftime("DUMP_%Y_%m_%d")
DATA_DUMP_DIR = '/home/orange/snowsql/snowflake-rowery/data/'

dirs = os.listdir('./data')


# Gets the version
ctx = snowflake.connector.connect(
    user='kajak',
    password='HasloDoWH1!',
    account='ep27219.switzerland-north.azure',
    )
cs = ctx.cursor()

try:
    CURRENT_DATABASE = 'FI'
    cs.execute(f'USE DATABASE TEST1')
    cs.execute(f'CREATE OR REPLACE SCHEMA {SCHEMA_DATE}')

    from codecs import open
    with open('create_tables.sql', 'r', encoding='utf-8') as f:
        for cur in ctx.execute_stream(f):
            for ret in cur:
                print(ret)

    cs.execute("CREATE OR REPLACE FILE FORMAT csv_format_1 TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;")
    cs.execute("CREATE OR REPLACE STAGE csv_stage_1 FILE_FORMAT = csv_format_1;")
    cs.execute(f'PUT file://{DATA_DUMP_DIR}{CURRENT_DATABASE}/*.csv @csv_stage_1 AUTO_COMPRESS=TRUE;')
    cs.execute("COPY INTO mytable FROM @csv_stage_1/ex.csv.gz FILE_FORMAT = (FORMAT_NAME = csv_format_1) ON_ERROR = 'skip_file';")
    cs.execute("REMOVE @csv_stage_1 PATTERN='.*.csv.gz';")
except Exception as e:
    print('b'+ str(e))
finally:
    cs.close()
ctx.close()