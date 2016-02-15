from pandas import *
from datetime import datetime
import httpagentparser

week_day_dict = {0:'Mon',1:'Tue',2:'Wed',3:'Thu',4:'Fri',5:'Sat',6:'Sun'}

def user_agent_parser(user_agent):

    temp = httpagentparser.detect(user_agent)

    try:
        os = temp['platform']['name'].replace(' ','_')
    except AttributeError:
        os = 'other'
    try:
        device = temp['dist']['name']
    except KeyError:
        device = temp['platform']['name']

    os = os.replace('iOS','Mac_OS').replace('Symbian','other').replace('BlackBerry','other')
    os = os.replace('PlayStation','other').replace('Nokia_S40','other')
    device = device.replace('Mac OS','Mac')
    device = device.replace('Windows','Desktop').replace('Linux','Desktop').replace('Ubuntu','Desktop').replace(' ChromeOS','Desktop')
    device = device.replace('Android','Mobile').replace('BlackberryPlaybook','Mobile').replace('BlackBerry','Mobile').replace('Symbian','Mobile')
    device = device.replace('PlayStation','other').replace('Debian','other').replace('BlackBerry','other').replace('Nokia S40','other')

    return os,device

def time_parser(date):

    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
    hours = date.hour
    month_day = date.day
    week_day = week_day_dict[date.weekday()]
    year_day = date.timetuple().tm_yday

    return hours,month_day,week_day,year_day

