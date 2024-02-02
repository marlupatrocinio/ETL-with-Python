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