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
        
        #create petl table from column arrays and rename the columns
        exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header = ['date', 'rate'])
        
        #load the expense doc
        try:
            expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx', sheet = 'Github')
        except Exception as e:
            print('Could not load expenses: ' + str(e))
            sys.exit()
            
        #join table
        expenses = petl.outerjoin(exchangeRates, expenses, key = 'date')
        
        #fill missing values
        expenses = petl.filldown(expenses, 'rate')
        
        #remove dates with no expenses
        expenses = petl.select(expenses, lambda rec: rec.USD != None)
        
        # add CDN column
        expenses = petl.addfield(expenses,'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

        try:
            dbConnection = pymssql.connect(server=destServer,database=destDatabase,user=user,password=password)
        except Exception as e:
            print('could not connect to database:' + str(e))
            sys.exit()

        # populate Expenses database table
        try:
            petl.io.todb (expenses,dbConnection,'Expenses')
        except Exception as e:
            print('could not write to database:' + str(e))
            
    print (expenses)