#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import os
import csv
from datetime import datetime
from pandas import DataFrame
import time
import json

old_time_modification = 0.0;
epoch = datetime.fromtimestamp(0)
c=0

while True:
    # getting last time of new_data.csv file modification 
    new_time_modification = os.stat('new_data.csv').st_mtime
    # compairing with the last time new_data.csv was edited 
    if new_time_modification != old_time_modification :
        # starting socket
        sock = socket.socket()
        sock.connect(('localhost', 9090))
        # reading new rows from file which were added after last time of modification
        df = []
        with open('new_data.csv', 'r') as textfile:
            try:
                for row in reversed(list(csv.reader(textfile))):
                    if (datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S.%f") - epoch).total_seconds() > old_time_modification:
                        df.append(row)
                        print (df)
                    else:
                        break
            except ValueError:
                pass
        textfile.close()
        # making new file modification time - old file modification time 
        old_time_modification = os.stat('new_data.csv').st_mtime
        # sending data on server
        print ('data sent')
        sock.send(str.encode(json.dumps(df),'utf-8'))
        # getting session_id, scored labels, scored probability
        print ('data starting receiving')
        data = sock.recv(8196)
        data = json.loads(data.decode('utf-8'))
        print ('data received')
        # adding it into prediction file
        with open('prediction.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(data)
        f.close()
#        print (DataFrame(data['Results']['result']['value']['Values'],columns = ['session_id','label','scored_prob']))
        print ('chages added')
    # if new_data.csv haven`t new data skip server part
    else:
        print ('no chages added')

    time.sleep(5)

sock.close()
