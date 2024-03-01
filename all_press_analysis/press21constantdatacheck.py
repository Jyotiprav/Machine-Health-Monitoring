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
pressdb = myclient_global['Press_21']

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

def p21constantdata():
    end = datetime.now()
    start = end - timedelta(hours = 2)

    #checks for constant data in batch 1

    errorlist = []

    QUERY = {'Date' : {'$gte' : start, '$lt' : end}}
    projection = {'_id': 0,  'Date': 1, **{batch_info_dict[1][x]: 1 for x in range(len(batch_info_dict[1]))}} #with Prssangle
    collection = pressdb['BATCH_1']
    results = collection.find(QUERY, projection)
    filtered_df = pd.DataFrame(results).set_index('Date')
    
    # df['angle_diff'] = df['Press_Angle'].diff()

    # significant_changes = df['angle_diff'].abs() > 0.001

    # filtered_df = df[significant_changes]
    # filtered_df = filtered_df.drop(["angle_diff", 'Press_Angle'], axis = 1)

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
    
    # df2['angle_diff'] = df2['Press_Angle'].diff()

    # significant_changes2 = df2['angle_diff'].abs() > 0.001

    # filtered_df2 = df2[significant_changes2]
    # filtered_df2 = filtered_df2.drop(["angle_diff", 'Press_Angle'], axis = 1)
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
    batch1 = [
        "Col1_Gibbs_Vrms",
        "Col1_Gibbs_Arms",
        "Col1_Gibbs_Apeak",
        "Col1_Gibbs_Crest",
        "Col1_Gibbs_Temp",
        "Col2_Gibbs_Vrms",
        "Col2_Gibbs_Arms",
        "Col2_Gibbs_Apeak",
        "Col2_Gibbs_Crest",
        "Col2_Gibbs_Temp",
        "Col3_Gibbs_Vrms",
        "Col3_Gibbs_Arms",
        "Col3_Gibbs_Apeak",
        "Col3_Gibbs_Crest",
        "Col3_Gibbs_Temp",
        "Col4_Gibbs_Vrms",
        "Col4_Gibbs_Arms",
        "Col4_Gibbs_Apeak",
        "Col4_Gibbs_Crest",
        "Col4_Gibbs_Temp",
        "Col1_QDC_Pre",
        "Col3_QDC_Pre",
        "Rm_LCNut_Vrms",
        "Rm_LCNut_Arms",
        "Rm_LCNut_Apeak",
        "Rm_LCNut_Crest",
        "Rm_LCNut_Temp",
        "Rm_AdjMtr_Vrms",
        "Rm_AdjMtr_Arms",
        "Rm_AdjMtr_Apeak",
        "Rm_AdjMtr_Crest",
        "Rm_AdjMtr_Temp",
        "Rm_AdjBrng_Vrms",
        "Rm_AdjBrng_Arms",
        "Rm_AdjBrng_Apeak",
        "Rm_AdjBrng_Crest",
        "Rm_AdjBrng_Temp",
        "Rm_RCNut_Vrms",
        "Rm_RCNut_Arms",
        "Rm_RCNut_Apeak",
        "Rm_RCNut_Crest",
        "Rm_RCNut_Temp",
        "Prs_MMtrCltch_Vrms",
        "Prs_MMtrCltch_Arms",
        "Prs_MMtrCltch_Apeak",
        "Prs_MMtrCltch_Crest",
        "Prs_MMtrCltch_Temp",
        "Prs_MMtr_Vrms",
        "Prs_MMtr_Arms",
        "Prs_MMtr_Apeak",
        "Prs_MMtr_Crest",
        "Prs_MMtr_Temp",
        "ClutchPre",
        "Cl_Br_SrgTankPre",
        "FlywhlBrake_Pre",
        "R_Cbal_RsrPre",
        "L_CBal_RsrPre",
        "BrakePre",
        "Prs_Main_AirPre",
        "ClutchFRL_AirPre",
        "CounterBalance_Pre"
    ]

    batch2 = [
        "LubeBlock1Pre",
        "RLIntrBrng_Vrms",
        "RLIntrBrng_Arms",
        "RLIntrBrng_Apeak",
        "RLIntrBrng_Crest",
        "RLIntrBrng_Temp",
        "RLEccBshg_Vrms",
        "RLEccBshg_Arms",
        "RLEccBshg_Apeak",
        "RLEccBshg_Crest",
        "RLEccBshg_Temp",
        "FrontLubeBlockPre",
        "FL_EccBshng_Vrms",
        "FL_EccBshng_Arms",
        "FL_EccBshng_Apeak",
        "FL_EccBshng_Crest",
        "FL_EccBshng_Temp",
        "FL_IntrBrng_Vrms",
        "FL_IntrBrng_Arms",
        "FL_IntrBrng_Apeak",
        "FL_IntrBrng_Crest",
        "FL_IntrBrng_Temp",
        "FRIntrBrng_Vrms",
        "FRIntrBrng_Arms",
        "FRIntrBrng_Apeak",
        "FRIntrBrng_Crest",
        "FRIntrBrng_Temp",
        "FREccBshng_Vrms",
        "FREccBshng_Arms",
        "FREccBshng_Apeak",
        "FREccBshng_Crest",
        "FREccBshng_Temp",
        "LubeBlock2Pre",
        "RREccBshg_Vrms",
        "RREccBshg_Arms",
        "RREccBshg_Apeak",
        "RREccBshg_Crest",
        "RREccBshg_Temp",
        "RRIntrBrng_Vrms",
        "RRIntrBrng_Arms",
        "RRIntrBrng_Apeak",
        "RRIntrBrng_Crest",
        "RRIntrBrng_Temp",
        "LubePump_Vrms",
        "LubePump_Arms",
        "LubePump_Apeak",
        "LubePump_Crest",
        "LubePump_Temp",
        "LubeMotor_Vrms",
        "LubeMotor_Arms",
        "LubeMotor_Apeak",
        "LubeMotor_Crest",
        "LubeMotor_Temp",
        "LubeRsrLevel_mm",
        "Prs_LubePostFilterPre",
        "Prs_LubePreFilterPre",
        "Rm_LubeBlckInPre",
        "Rm_LeftLwrLnk_Pre",
        "Rm_RightLwrLnk_Pre"
    ]
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