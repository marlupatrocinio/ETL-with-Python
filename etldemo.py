import os
import petl
import pymssql
import configparser
import requests
import datetime
import json
import decimal

#get data from config file
config = configparser.ConfigParser()
try:
    config.read('ETLDemo.ini')
except Exception as e:
    print('Error reading config file: ' + str(e))
    sys.exit()
    
#read settings from config file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

#request data from url
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('Could not make request: ' + str(e))
    sys.exit()

#initialize list for data storage later
BOCDates = []
BOCRates = []

#check response status code and process json
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)
 
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))
        
