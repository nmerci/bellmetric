#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import socket
import json
import urllib
import zipfile
from scheme import time_parser, user_agent_parser  
# reading visitor history file
archive = zipfile.ZipFile('visitors_history.zip')
visitor_history = pd.read_csv(archive.open('visitors_history.csv'))
visitor_history.index = visitor_history.visitor_id

while True:
    # starting socket
    sock = socket.socket()
    sock.bind(('', 9090))
    sock.listen(1)
    conn, addr = sock.accept()
    print ('connected:', addr)
    # receiving data
    data = conn.recv(4096)
    if not data:
        break
    data = json.loads(data.decode('utf-8'))
    df = pd.DataFrame(data,columns = ['visitor_id', 'session_id', 'time', 'page_id', 'source_id', 'user_agent', 'is_checkout_page', 'cc', 'cloc']).replace('',0)
    df = df.replace('nan',0)
    values = []
    # aggreagting data to model
    for row in df.index:
        s_id = df.session_id[row]
        v_id = df.visitor_id[row]
            # нюанс нам не нужно прдсказывать по всем приходящим данным, нужны только первые ивенты в сессии
        hour, month_day, week_day, year_day = time_parser(df.time[row])
            # history of users searching
        try:
            h_session_number, h_events_number, h_checkout_number, h_mean_session_time = list(visitor_history[df.visitor_id[row]])
        except KeyError:
            h_session_number, h_events_number, h_checkout_number, h_mean_session_time = [0,0,0,0]
        # taking os and device type
        os, device = user_agent_parser(df.user_agent[row])
        # country, sourse_id, page_id
        ### needed city to add ###
        country = df['cc'][row]
        source_id = int(float(df['source_id'][row]))
        page_id = df['page_id'][row]
        # making list of list of values for model
        values.append([str(s_id),str(v_id),str(country),str(hour),str(week_day),str(month_day),str(year_day),
                       str(h_session_number), str(h_events_number), str(h_checkout_number), str(h_mean_session_time),
                       str(os),str(device),str(page_id),str(source_id)])
        # print (s_id)
    # importing data in azure ml
    data =  { "Inputs" : { "input1" : {
                           "ColumnNames": ["session_id", "visitor_id", "country", "hour", "week_day", "month_day", "year_day", "h_session_number", "h_events_number", "h_checkout_number", "h_mean_session_time", "os", "device", "page_id", "source_id"],
                           "Values": [ values[i] for i in range(0,len(values)) ]
                         },           },
              "GlobalParameters": {}  }
    # print (data['Inputs']['input1']['Values'])
            
    body = str.encode(json.dumps(data))
    url = 'https://ussouthcentral.services.azureml.net/workspaces/ef36850ee70a4ee1ac2c26847293268d/services/d02649f4bbb643c98d93cd8f7ecd0210/execute?api-version=2.0&details=true'
    api_key = 'b9FiTwZ2a0ABO25aNO2NcbveZtAITE11/t05MW5xujbpr0RnHucfGX8EvfKv785oVcP9z9XNXA3BK0nzgSbYkQ=='
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
    req = urllib.request.Request(url, body, headers) 
          
    try:
        # getting results from azure
        response = urllib.request.urlopen(req)
        result = json.loads(response.read().decode('utf-8'))
        # sending data to client
        conn.send(str.encode(json.dumps(result['Results']['result']['value']['Values']),'utf-8'))
        print (str.encode(json.dumps(result['Results']['result']['value']['Values']),'utf-8'))
    except urllib.error.HTTPError as err:
        # possible error on https://msdn.microsoft.com/en-us/library/azure/dn913081.aspx
        print("The request failed with status code: " + str(err.code))
        print(err.info())
        print(json.loads(err.read().decode('utf-8')))            
        conn.close()
    conn.close()