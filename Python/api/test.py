import pandas as pd
import csv
import zipfile
import time
from datetime import datetime
rawdata = pd.read_csv('test_data.csv')

while True:

    for i in rawdata.index:
        towrite = list(rawdata.ix[i])
        towrite[2] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S.%f")
        print ('adding 1 row')
        with open('new_data.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(towrite)
        f.close()
        time.sleep(5)

