import time
from pymongo.errors import ConnectionFailure
from pymongo import MongoClient
from twilio.rest import Client
import smtplib
from email.mime.text import MIMEText
from email.message import EmailMessage
import ssl
import datetime
import numpy as np
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import pyplot
from datetime import datetime
import seaborn as sns
from datetime import timedelta

sender_email = 'pressshop.stats@gmail.com'
app_password = 'vabelwauzapvxnrq'
recipient_email = [
    'eric.li@martinrea.com'
    ]

myclient_global = pymongo.MongoClient(host = "128.121.34.13",connect = True)
pressdb = myclient_global['Press_05']

column_list = []

#Collects all features from all 4 batches

allkeysbatch1 = pressdb.BATCH_1.find().limit(1).sort("_id",-1)
for x in [allkeysbatch1[0].keys()]:
    column_list += x

allkeysbatch2 = pressdb.BATCH_2.find().limit(1).sort("_id",-1)
for x in [allkeysbatch2[0].keys()]:
    column_list += x

#Puts all features in a list except for press angle, data and id

column_list1 = []
for x in range(len(column_list)-1):
    if column_list[x] != 'Date' and column_list[x] != '_id' and column_list[x] != 'Press_Angle':
        column_list1 += [column_list[x]]

batch_info_dict = {}

#puts all features in a dictionary with respective batch

for column in column_list1:
    for batch in [1,2]:
        collection = "BATCH_"+str(batch)
        db = pressdb[collection]
        results = db.find().limit(1).sort("_id",-1)
        recent_doc = results[0]
        if column in recent_doc.keys():
            if batch not in batch_info_dict:
                batch_info_dict[batch] = [column]
            else:
                batch_info_dict[batch].append(column)
batch_info_dict[list(batch_info_dict.keys())[0]]

def press05constantdata():
    end = datetime.now()
    start = end - timedelta(hours = 2)

    #checks for constant data in batch 1

    errorlist = []

    QUERY = {'Date' : {'$gte' : start, '$lt' : end}}
    projection = {'_id': 0,  'Date': 1, **{batch_info_dict[1][x]: 1 for x in range(len(batch_info_dict[1]))}} #with Prssangle
    collection = pressdb['BATCH_1']
    results = collection.find(QUERY, projection)
    filtered_df = pd.DataFrame(results).set_index('Date')
    

    if filtered_df.empty:
        print('Press not running')
    else:
        filtered_df_diff = filtered_df.diff().abs()
        sum_diff = filtered_df_diff.sum(axis=0)
        for x in batch_info_dict[1]:
            if sum_diff[x] == 0:
                errorlist += [x]
    #==============================================================================================================================       
    QUERY = {'Date' : {'$gte' : start, '$lte' : end}}
    projection = {'_id': 0,  'Date': 1, **{batch_info_dict[2][x]: 1 for x in range(len(batch_info_dict[2]))}} #with Press_angle
    collection = pressdb['BATCH_2']
    results = collection.find(QUERY, projection)
    filtered_df2 = pd.DataFrame(results).set_index('Date')

    if filtered_df2.empty:
        print('Press not running')
    else:
        filtered_df_diff2 = filtered_df2.diff().abs()
        sum_diff2 = filtered_df_diff2.sum(axis=0)
        for x in batch_info_dict[2]:
            if sum_diff2[x] == 0:
                errorlist += [x]
    #======================================================================================
    #checks if there are any sensors in error list indicating constant data
    if len(errorlist) == 0:
        print('No constant data errors')
    else:
        print(errorlist)
        print("These sensors have constant data for the last 2 hours")
        print()
    #==============================================================================================================================================================
    #NaN data check
    batch1=['R_Wst_L_Pin_Vrms', 
            'R_Wst_L_Pin_Apeak', 
            'R_Wst_L_Pin_Arms', 
            'R_Wst_L_Pin_Crest', 
            'R_Wst_L_Pin_Temp', 
            'W_Axis_Vrms', 
            'W_Axis_Apeak', 
            'W_Axis_Arms', 
            'W_Axis_Crest', 
            'W_Axis_temp', 
            'R_Wst_U_Pin_Vrms', 
            'R_Wst_U_Pin_Apeak', 
            'R_Wst_U_Pin_Arms', 
            'R_Wst_U_Pin_Crest', 
            'R_Wst_U_Pin_Temp', 
            'R_Fly_Brng_Vrms', 
            'R_Fly_Brng_Apeak', 
            'R_Fly_Brng_Arms', 
            'R_Fly_Brng_Crest', 
            'R_Fly_Brng_Temp', 
            'S_Axis_Vrms', 
            'S_Axis_Apeak', 
            'S_Axis_Arms', 
            'S_Axis_Crest', 
            'S_Axis_Temp', 
            'MM_Vrms', 
            'MM_Apeak', 
            'MM_Arms', 
            'MM_Crest', 
            'MM_temp', 
            'R_Est_L_Pin_Vrms', 
            'R_Est_L_Pin_Apeak', 
            'R_Est_L_Pin_Arms', 
            'R_Est_L_Pin_Crest', 
            'R_Est_L_Pin_Temp', 
            'R_Est_U_pin_Vrms', 
            'R_Est_U_pin_Apeak', 
            'R_Est_U_pin_Arms', 
            'R_Est_U_pin_Crest', 
            'R_Est_U_pin_Temp']

    batch2 =['F_Wst_L_Pin_Vrms', 
             'F_Wst_L_Pin_Apeak', 
             'F_Wst_L_Pin_Arms', 
             'F_Wst_L_Pin_Crest', 
             'F_Wst_L_Pin_Temp', 
             'N_Axis_Vrms',
             'N_Axis_Apeak', 
             'N_Axis_Arms', 
             'N_Axis_Crest', 
             'N_Axis_Temp', 
             'F_Wst_U_Pin_Vrms', 
             'F_Wst_U_Pin_Apeak', 
             'F_Wst_U_Pin_Arms', 
             'F_Wst_U_Pin_Crest', 
             'F_Wst_U_Pin_Temp', 
             'F_Fly_Brng_Vrms', 
             'F_Fly_Brng_Apeak', 
             'F_Fly_Brng_Arms', 
             'F_Fly_Brng_Crest', 
             'F_Fly_Brng_Temp', 
             'F_Mid_Pin_Vrms', 
             'F_Mid_Pin_Apeak', 
             'F_Mid_Pin_Arms', 
             'F_Mid_Pin_Crest', 
             'F_Mid_Pin_Temp', 
             'F_Est_L_Pin_Vrms', 
             'F_Est_L_Pin_Apeak', 
             'F_Est_L_Pin_Arms', 
             'F_Est_L_Pin_Crest', 
             'F_Est_L_Pin_Temp', 
             'F_Est_U_Pin_Vrms', 
             'F_Est_U_Pin_Apeak',
             'F_Est_U_Pin_Arms', 
             'F_Est_U_Pin_Crest', 
             'F_Est_U_Pin_Temp', 
             'E_Axis_Vrms', 
             'E_Axis_Apeak', 
             'E_Axis_Arms', 
             'E_Axis_Crest', 
             'E_Axis_Temp']
    nan_list = []
    batch1collection = pressdb['BATCH_1']
    batch2collection = pressdb['BATCH_2']
    #finds the most recent batch 1 and 2 documents
    batch1documents = batch1collection.find().sort('_id', -1).limit(1)
    batch2documents = batch2collection.find().sort('_id', -1).limit(1)

    recentbatch1document = batch1documents[0]
    recentbatch2document = batch2documents[0]

    #loops through to check if all sensors are in the most recent document
    #sensor missing implies no data

    for sensor in batch1:
        if sensor not in recentbatch1document:
            nan_list += [sensor]
        
    for sensor in batch2:
        if sensor not in recentbatch2document:
            nan_list += [sensor]

    #checks to see if there are any sensors with no data and sends out an email with sensors with no data

    if len(nan_list) == 0:
        print('No NaN Data errors')
    
    else:
        error_msg = ''
        for sensor in nan_list:
            error_msg +=  str(sensor) + ', '
        error_msg += 'has no data coming in.'
        print(error_msg)
