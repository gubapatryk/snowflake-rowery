from datetime import datetime
import os, sys


dirs = os.listdir('./data')
print(dirs)
for dir in dirs:
    print(dir)
now = datetime.now()
 
dt_string = now.strftime("DUMP-%Y-%m-%d")
print("date and time =", dt_string)

