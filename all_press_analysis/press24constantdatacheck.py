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
pressdb = myclient_global['Press_24']

column_list = []

#Collects all features from all 4 batches

allkeysbatch1 = pressdb.BATCH_1.find().limit(1).sort("_id",-1)
for x in [allkeysbatch1[0].keys()]:
    column_list += x

allkeysbatch2 = pressdb.BATCH_2.find().limit(1).sort("_id",-1)
for x in [allkeysbatch2[0].keys()]:
    column_list += x

allkeysbatch3 = pressdb.BATCH_3.find().limit(1).sort("_id",-1)
for x in [allkeysbatch3[0].keys()]:
    column_list += x

allkeysbatch4 = pressdb.BATCH_4.find().limit(1).sort("_id",-1)
for x in [allkeysbatch4[0].keys()]:
    column_list += x

#Puts all features in a list except for press angle, data and id

column_list1 = []
for x in range(len(column_list)-1):
    if column_list[x] != 'Date' and column_list[x] != '_id' and column_list[x] != 'Press_Angle':
        column_list1 += [column_list[x]]

batch_info_dict = {}

#puts all features in a dictionary with respective batch

for column in column_list1:
    for batch in [1,2,3,4]:
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

def p24constantdata():
    end = datetime.now()
    start = end - timedelta(hours = 2)

    #checks for constant data in batch 1

    errorlist = []

    QUERY = {'Date' : {'$gte' : start, '$lt' : end}}
    projection = {'_id': 0, 'Press_Angle': 1, 'Date': 1, **{batch_info_dict[1][x]: 1 for x in range(len(batch_info_dict[1]))}}
    collection = pressdb['BATCH_1']
    results = collection.find(QUERY, projection)
    df = pd.DataFrame(results).set_index('Date')
    
    df['angle_diff'] = df['Press_Angle'].diff()

    significant_changes = df['angle_diff'].abs() > 0.001

    filtered_df = df[significant_changes]
    filtered_df = filtered_df.drop(["angle_diff", 'Press_Angle'], axis = 1)

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
    projection = {'_id': 0, 'Press_Angle': 1, 'Date': 1, **{batch_info_dict[2][x]: 1 for x in range(len(batch_info_dict[2]))}}
    collection = pressdb['BATCH_2']
    results = collection.find(QUERY, projection)
    df2 = pd.DataFrame(results).set_index('Date')
    
    df2['angle_diff'] = df2['Press_Angle'].diff()

    significant_changes2 = df2['angle_diff'].abs() > 0.001

    filtered_df2 = df2[significant_changes2]
    filtered_df2 = filtered_df2.drop(["angle_diff", 'Press_Angle'], axis = 1)
    if filtered_df2.empty:
        print('Press not running')
    else:
        filtered_df_diff2 = filtered_df2.diff().abs()
        sum_diff2 = filtered_df_diff2.sum(axis=0)
        for x in batch_info_dict[2]:
            if sum_diff2[x] == 0:
                errorlist += [x]
    #=============================================================================================================================
    QUERY = {'Date' : {'$gte' : start, '$lte' : end}}
    projection = {'_id': 0, 'Press_Angle': 1, 'Date': 1, **{batch_info_dict[3][x]: 1 for x in range(len(batch_info_dict[3]))}}
    collection = pressdb['BATCH_3']
    results = collection.find(QUERY, projection)
    df3 = pd.DataFrame(results).set_index('Date')
    
    df3['angle_diff'] = df3['Press_Angle'].diff()

    significant_changes3 = df3['angle_diff'].abs() > 0.001

    filtered_df3 = df3[significant_changes3]
    filtered_df3 = filtered_df3.drop(["angle_diff", 'Press_Angle'], axis = 1)
    if filtered_df3.empty:
        print('Press not running')
    else:
        filtered_df_diff3 = filtered_df3.diff().abs()
        sum_diff3 = filtered_df_diff3.sum(axis=0)
        for x in batch_info_dict[3]:
            if sum_diff3[x] == 0:
                errorlist += [x]
    #=============================================================================================================================
    QUERY = {'Date' : {'$gte' : start, '$lte' : end}}
    projection = {'_id': 0, 'Press_Angle': 1, 'Date': 1, **{batch_info_dict[4][x]: 1 for x in range(len(batch_info_dict[4]))}}
    collection = pressdb['BATCH_4']
    results = collection.find(QUERY, projection)
    df4 = pd.DataFrame(results).set_index('Date')
    
    df4['angle_diff'] = df4['Press_Angle'].diff()

    significant_changes4 = df4['angle_diff'].abs() > 0.001

    filtered_df4 = df4[significant_changes4]
    filtered_df4 = filtered_df4.drop(["angle_diff", 'Press_Angle'], axis = 1)
    if filtered_df4.empty:
        print('Press not running')
    else:
        filtered_df_diff4 = filtered_df4.diff().abs()
        sum_diff4 = filtered_df_diff4.sum(axis=0)
        for x in batch_info_dict[4]:
            if sum_diff4[x] == 0:
                errorlist += [x]
    #================================================================================
    #checks if there are any sensors in error list indicating constant data
    if len(errorlist) == 0:
        print('No constant data errors')
    else:
        print(errorlist)
        print("These sensors have constant data for the last 2 hours")
        print()
    #==============================================================================================================================================================
    #ALL BATCH SENSORS TO TEST IF ANY OF THEM ARE NOT PRESENT IN BATCHES
    batch1 = [
        "FL_UprLink_Pin_Vrms",
        "FL_UprLink_Pin_Arms",
        "FL_UprLink_Pin_Apk",
        "FL_UprLink_Pin_Crst",
        "FL_UprLink_Pin_Temp",
        "FL_Eccntric_Pin_Vrms",
        "FL_Eccntric_Pin_Arms",
        "FL_Eccntric_Pin_Apk",
        "FL_Eccntric_Pin_Crst",
        "FL_Eccntric_Pin_TempFar",
        "Frnt_InterPin_Vrms",
        "Frnt_InterPin_Apk",
        "Frnt_InterPin_Arms",
        "Frnt_InterPin_Crst",
        "Frnt_InterPin_Temp",
        "Drive_Shaft_Pin_Vrms",
        "Drive_Shaft_Pin_Arms",
        "Drive_Shaft_Pin_Apk",
        "Drive_Shaft_Pin_Crst",
        "Drive_Shaft_Pin_Temp",
        "FR_Eccntric_Pin_Vrms",
        "FR_Eccntric_Pin_Arms",
        "FR_Eccntric_Pin_Apk",
        "FR_Eccntric_Pin_Crst",
        "FR_Eccntric_Pin_Temp",
        "FR_UprLink_Pin_Vrms",
        "FR_UprLink_Pin_Arms",
        "FR_UprLink_Pin_Apk",
        "FR_UprLink_Pin_Crst",
        "FR_UprLink_Pin_Temp",
        "Ram_FR_CNut_Vrms",
        "Ram_FR_CNut_Arms",
        "Ram_FR_CNut_Apk",
        "Ram_FR_CNut_Crst",
        "Ram_FR_CNut_Temp",
        "Ram_FL_CNut_Vrms",
        "Ram_FL_CNut_Arms",
        "Ram_FL_CNut_Apk",
        "Ram_FL_CNut_Crst",
        "Ram_FL_CNut_Temp",
        "Ram_RR_CNut_Vrms",
        "Ram_RR_CNut_Arms",
        "Ram_RR_CNut_Apk",
        "Ram_RR_CNut_Crst",
        "Ram_RR_CNut_Temp",
        "Ram_RL_CNut_Vrms",
        "Ram_RL_CNut_Arms",
        "Ram_RL_CNut_Apk",
        "Ram_RL_CNut_Crst",
        "Ram_RL_CNut_Temp",
    ]

    batch2 = [
        "Crwn_HydUnit_VacIn",
        "Crwn_HydUnit_VacOut",
        "RadMtr_Vrms",
        "RadMtr_Arms",
        "RadMtr_Apk",
        "RadMtr_Crst",
        "RadMtr_Temp",
        "Crwn_HydUnit_Resvr_OilLvl",
        "Crwn_HydUnit_HydClutch_Pre",
        "Crwn_HydUnit_HydraulicPre",
        "Crwn_HydUnit_HydMtrVrms",
        "Crwn_HydUnit_HydMtrArms",
        "Crwn_HydUnit_HydMtrApk",
        "Crwn_HydUnit_HydMtrCrst",
        "Crwn_HydUnit_HydMtrTemp",
        "Crwn_HydUnit_HydPmp_Vrms",
        "Crwn_HydUnit_HydPmp_Arms",
        "Crwn_HydUnit_HydPmp_Apk",
        "Crwn_HydUnit_HydPmp_Crst",
        "Crwn_HydUnit_HydPmp_Temp",
        "CBal_pre_PSI",
        "Xfer_MainPre_PSI",
        "QDC_Drive_Pre_PSI",
        "FL_Gb_Cnd_Vrms",
        "FL_Gb_Cnd_Arms",
        "FL_Gb_Cnd_Apk",
        "FL_Gb_Cnd_Crst",
        "FL_Gb_Cnd_Temp",
        "FR_Gb_Cnd_Vrms",
        "FR_Gb_Cnd_Arms",
        "FR_Gb_Cnd_Apk",
        "FR_Gb_Cnd_Crst",
        "FR_Gb_Cnd_Temp",
        "RL_Gb_Cnd_Vrms",
        "RL_Gb_Cnd_Arms",
        "RL_Gb_Cnd_Apk",
        "RL_Gb_Cnd_Crst",
        "RL_Gb_Cnd_Temp",
        "RR_Gb_Cnd_Vrms",
        "RR_Gb_Cnd_Arms",
        "RR_Gb_Cnd_Apk",
        "RR_Gb_Cnd_Crst",
        "RR_Gb_Cnd_Temp",
        "Ram_FR_CNut_Lube_Pre",
        "Ram_FL_CNut_Lube_Pre",
        "Ram_RR_CNut_Lube_Pre",
        "Ram_RL_CNut_Lube_Pre",
        "RL_Ecc_Lube_Pre",
        "RL_Pitman_Lube_Pre",
        "FL_Ecc_Lube_Pre",
        "FL_Pitman_Lube_Pre",
        "RR_Ecc_Lube_Pre",
    ]

    batch3 = [
        "Ram_Adj_Mtr_Vrms",
        "Ram_Adj_Mtr_Arms",
        "Ram_Adj_Mtr_Apk",
        "Ram_Adj_Mtr_Crest",
        "Ram_Adj_Mtr_Temp",
        "RR_EccntricBshg_Vrms",
        "RR_EccntricBshg_Arms",
        "RR_EccntricBshg_Apk",
        "RR_EccntricBshg_Crst",
        "RR_EccntricBshg_Temp",
        "RR_UprLink_Bshg_Vrms",
        "RR_UprLink_Bshg_Arms",
        "RR_UprLink_Bshg_Apk",
        "RR_UprLink_Bshg_Crst",
        "RR_UprLink_Bshg_Temp",
        "FlyWhlShaftBshg_Outside_Vrms",
        "FlyWhlShaftBshg_Outside_Arms",
        "FlyWhlShaftBshg_Outside_Apk",
        "FlyWhlShaftBshg_Outside_Crst",
        "FlyWhlShaftBshg_Outside_Temp",
        "M_Mtr_Vrms",
        "M_Mtr_Arms",
        "M_Mtr_Apk",
        "M_Mtr_Crst",
        "M_Mtr_Temp",
        "R_I_P_Bshg_Vrms",
        "R_I_P_Bshg_Arms",
        "R_I_P_Bshg_Apk",
        "R_I_P_Bshg_Crst",
        "R_I_P_Bshg_Temp",
        "Rear_LubeBlock_Pre",
        "FR_Pitman_Lube_Pre",
        "FR_Ecc_Lube_Pre",
        "RL_EccntricBshg_Vrms",
        "RL_EccntricBshg_Arms",
        "RL_EccntricBshg_Apk",
        "RL_EccntricBshg_Crst",
        "RL_EccntricBshg_Temp",
        "RL_UprLink_Bshg_Vrms",
        "RL_UprLink_Bshg_Apk",
        "RL_UprLink_Bshg_Arms",
        "RL_UprLink_Bshg_Crst",
        "RL_UprLink_Bshg_Temp",
        "Xfer_HydUnit_OilLvl_UGT526inmm",
        "Xfer_UnitHydMtr_Vrms",
        "Xfer_UnitHydMtr_Arms",
        "Xfer_UnitHydMtr_Apk",
        "Xfer_UnitHydMtr_Crst",
        "Xfer_UnitHydMtr_Temp",
        "Stnr_Pinch_Roller_Pre",
        "Stnr_Mtr_Vrms",
        "Stnr_Mtr_Arms",
        "Stnr_Mtr_Apk",
        "Stnr_Mtr_Crst",
        "Stnr_Mtr_Temp",
    ]

    batch4 = [
        "RR_Pitman_Lube_Pre",
        "DieClamp1_Pre",
        "DieClamp2_Pre",
        "QDC_UnClamp_Pre",
        "QDC_Clamp_Pre",
        "QDC_Lift_Pre",
        "MainHPU_Pre",
        "BlstrHPU_OilLvl",
        "Pnmatic_Driving_Pre",
        "BGr_Bshg_Vrms",
        "BGr_Bshg_Arms",
        "BGr_Bshg_Apk",
        "BGr_Bshg_Crst",
        "BGr_Bshg_Temp",
        "FwhlShaftBshg_In_Vrms",
        "FwhlShaftBshg_In_Apk",
        "FwhlShaftBshg_In_Arms",
        "FwhlShaftBshg_In_Crst",
        "FwhlShaftBshg_In_Temp",
        "RCBal_Resvr_Pre",
        "LCBal_Resvr_Pre_PSI",
        "uncolr_Mandrel_ret_Pre",
        "uncolr_Mandrel_Exp_Pre",
        "uncolr_Mandrel_JgFwd_Pre",
        "uncolr_Mandrel_JgRev_Pre",
        "uncolr_ClutchnBrake_Pre",
        "Pit_Hyd_Prefilter_Real_PSI",
        "Pit_HydPre_Postfilter_Real_PSI",
        "Pit_HydOverload_Pre_Real_PSI",
        "Pit_Pmp_Vrms",
        "Pit_Pmp_Arms",
        "Pit_Pmp_Apk",
        "Pit_Pmp_Crst",
        "Pit_Pmp_Temp",
        "Pit_LubeLevel",
        "Pit_Mtr_Vrms",
        "Pit_Mtr_Arms",
        "Pit_Mtr_Apk",
        "Pit_Mtr_Crst",
        "Pit_Mtr_Temp",
        "PitFlood",
        "Fdr_Pnch_Rler_Pnmatic_Pre",
        "Fdr_UpDwn_Pnmatic_Pre",
        "XferHyd_Pre",
        "Fdr_Guide_Pre",
        "Stnr_Pmp_HydPre",
        "Stnr_Pmp_Vrms",
        "Stnr_Pmp_Arms",
        "Stnr_Pmp_Apk",
        "Stnr_Pmp_Crst",
        "Stnr_Pmp_Temp",
        "Stnr_HydResvr_OilLvl",
        "Stnr_MandrelArmRoller_FwdPre",
        "Stnr_MandrelArmRoller_RevPre",
        "Stnr_HydMtrVrms",
        "Stnr_HydMtrArms",
        "Stnr_HydMtrApk",
        "Stnr_HydMtrCrst",
        "Stnr_HydMtrTemp",
        "CrwnFr_LBlockPre",
    ]

    nan_list = []

    #sets batches
    batch1collection = pressdb["BATCH_1"]
    batch2collection = pressdb["BATCH_2"]
    batch3collection = pressdb["BATCH_3"]
    batch4collection = pressdb["BATCH_4"]
#finds the most recent batch 1 and 2 documents
    batch1documents = batch1collection.find().sort('_id', -1).limit(1)
    batch2documents = batch2collection.find().sort('_id', -1).limit(1)
    batch3documents = batch3collection.find().sort('_id', -1).limit(1)
    batch4documents = batch4collection.find().sort('_id', -1).limit(1)

    recentbatch1document = batch1documents[0]
    recentbatch2document = batch2documents[0]
    recentbatch3document = batch3documents[0]
    recentbatch4document = batch4documents[0]

        #loops through to check if all sensors are in the most recent document
        #sensor missing implies no data

    for sensor in batch1:
        if sensor not in recentbatch1document:
            nan_list += [sensor]
        
    for sensor in batch2:
        if sensor not in recentbatch2document:
            nan_list += [sensor]
    
    for sensor in batch3:
        if sensor not in recentbatch3document:
            nan_list += [sensor]
        
    for sensor in batch4:
        if sensor not in recentbatch4document:
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
        