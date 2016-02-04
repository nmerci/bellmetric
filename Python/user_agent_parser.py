# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 15:43:58 2016

@author: Niko
"""
import zipfile
from pandas import *
import numpy as np
import csv

archive = zipfile.ZipFile('c:/users/prost/bellmetric/data/session_data.zip')
data = read_csv(archive.open('session_data.csv'))

# extracting os name from user_agent column

os_list = []
for row in data.index:
    print (row)
    try:
        os = data['user_agent'][row].split('0 (')[1].split(') ')[0].split(';')[0].split(' ')[0]
        if os == 'compatible':
            os = data['user_agent'][row].split('MSIE')[1].split('; ')[1].split(';')[0].split(' ')[0]
            os_list.append(os)
        elif os=='Mobile':
            os = data['user_agent'][row].split('Mobile; ')[1].split(' ')[0]
            os_list.append(os)
        else:
            os_list.append(os)
    except IndexError:
        os_list.append(np.nan)
    except AttributeError:
        os_list.append(np.nan)

data['os'] = Series(os_list,name = 'os')

other_os = set(unique(os_list)) - set(['Windows','Macintosh','iPad','iPhone','Android','iPod','Linux'])

data.os = data.os.replace('Linux','Android')
for name in other_os:
    data.os = data.os.replace(name,'other')
for name in ['Macintosh','iPad','iPhone','iPod']:
    data.os = data.os.replace(name,'Mac_iOS')

# extracting version of os from user_agent column

version_list = []
for row in data.index:
    print (row)
    try:
        if 'NT' in data['user_agent'][row]:
            version = data['user_agent'][row].split('Windows ')[1].split(';')[0].split(')')[0]
        elif 'Mac' in data['user_agent'][row]:
            version = data['user_agent'][row].split('OS ')[1].split(') ')[0].replace(' like Mac ','').split(';')[0]
        elif 'Linux' in data['user_agent'][row]:
            version = data['user_agent'][row].split('Linux; ')[1].replace('U; ','').split(';')[0].replace('Android ','')
        else:
            version = 'other'
        version_list.append(version)
    except IndexError:
        version_list.append(np.nan)
    except AttributeError:
        version_list.append(np.nan)
    except TypeError:
        version_list.append(np.nan)

data['os_version'] = Series(version_list,name = 'os_version')

filtered_version = Series(version_list).value_counts()[Series(version_list).value_counts()<=32]
other_version = [len(name) > 15 for name in filtered_version.index]
other_version = list(filtered_version[other_version].index)
other_version += ['webOS/2.2.4','NT 5.1 88XI4n5O','U','X','NetCast']

for name in other_version:
    data.os_version = data.os_version.replace(name,'other')

# extracting device type from user_agent column

device_list = []
for row in data.index:
    print (row)
    try:
        device = data['user_agent'][row].split('0 (')[1].split(') ')[0].split(';')[0].split(' ')[0]
        device_list.append(device)
    except IndexError:
        device_list.append(np.nan)
    except AttributeError:
        device_list.append(np.nan)

data['device'] = Series(device_list,name = 'device')

other_device = set(unique(device_list)) - set(['Windows','Macintosh','iPad','iPhone','Android','iPod','Linux','Mobile','compatible','BB10'])
data.device = data.device.replace('Linux','Phone')
data.device = data.device.replace('Android','Phone')
data.device = data.device.replace('compatible','Windows')
data.device = data.device.replace('Mobile','Phone')
data.device = data.device.replace('BB10','Phone')
data.device = data.device.replace('Macintosh','MacBook')

for name in other_device:
    data.device = data.device.replace(name,'other')

##### making history of users for each session

raw_data = read_csv('c:/preprocessed_data.csv')
raw_data = raw_data[['session_id','visitor_id','seconds_since_epoch','is_checkout_page']]

by_session = pivot_table(raw_data[['visitor_id','session_id','seconds_since_epoch']],index = ['session_id','visitor_id'],values = ['seconds_since_epoch'],aggfunc = [max,min,len]).reset_index()
by_session.columns = ['session_id','visitor_id','last_event_time','first_event_time','number_of_events']
by_session['time_on_site'] = by_session['last_event_time'] - by_session['first_event_time']

checkouts = pivot_table(raw_data[['session_id','is_checkout_page']],
                         index = ['session_id'],
                         values = ['is_checkout_page'],
                         aggfunc = sum).reset_index()
checkouts = checkouts.rename(columns ={'is_checkout_page':'checkouts'})
by_session = merge(by_session,checkouts)

by_session = by_session.sort(['visitor_id','first_event_time'],axis=0).reset_index(drop = True)

hist_event_number = [0]
hist_session_number = [0]
hist_checkout_number = [0]
hist_mean_session_time = [0]

c=1
for row in by_session.index[1:]:
    print (row)
    if by_session.visitor_id[row] == by_session.visitor_id[row - 1]:
        hist_session_number.append(c)
        hist_event_number.append(sum(by_session['number_of_events'][row-c:row]))
        hist_checkout_number.append(sum(by_session['checkouts'][row-c:row]))
        hist_mean_session_time.append(round(np.mean(by_session['time_on_site'][row-c:row]),2))
        c+=1
    else:
        hist_session_number.append(0)
        hist_event_number.append(0)
        hist_checkout_number.append(0)
        hist_mean_session_time.append(0)
        c = 1
 
by_session['h_session_number'] = Series(hist_session_number).astype(int)
by_session['h_events_number'] = Series(hist_event_number).astype(int)
by_session['h_checkout_number'] = Series(hist_checkout_number).astype(int)
by_session['h_mean_session_time'] = Series(hist_mean_session_time)

by_session = by_session.drop(['visitor_id', 'last_event_time', 'first_event_time',
       'number_of_events', 'time_on_site', 'checkouts'],axis=1)

df = merge(data,by_session)

df['device'] = df['device'].replace('Windows','Desktop')
df['h_mean_session_time'] = df['h_mean_session_time'].astype(int)

df.to_csv('c:/users/prost/session_data.csv',index = False,encoding='utf-8',quoting = csv.QUOTE_NONNUMERIC,quotechar='"')