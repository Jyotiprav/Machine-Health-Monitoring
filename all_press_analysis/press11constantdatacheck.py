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
from datetime import time as dttime
import seaborn as sns
from datetime import timedelta

sender_email = 'pressshop.stats@gmail.com'
app_password = 'vabelwauzapvxnrq'
recipient_email = [
    'eric.li@martinrea.com'
    ]

myclient_global = pymongo.MongoClient(host = "128.121.34.13",connect = True)
pressdb = myclient_global['Press_11']

#=========================================================================================
#batch lists for no data check

batch1 = [
    'Cl_Br_Pre_psi',
    'CBal_Pre_psi',
    'Main_Air_Pre_psi',
    'Ram_Adjust_Chain_On/Off',
    'Lubrication_B_05_Pressure_psi',
    'F_L_Nut_Vrms',
    'F_L_Nut_Arms',
    'F_L_Nut_Temp',
    'F_R_Nut_Vrms',
    'F_R_Nut_Arms',
    'F_R_Nut_Temp',
    'R_L_Nut_Vrms',
    'R_L_Nut_Arms',
    'R_L_Nut_Temp',
    'R_R_Nut_Vrms',
    'R_R_Nut_Arms',
    'R_R_Nut_Temp',
    'Ram_Adjust_Vrms',
    'Ram_Adjust_Arms',
    'Ram_Adjust_Temp',
    'M_M_Vrms',
    'M_M_Arms',
    'M_M_Temp',
    'Cl_Vrms',
    'Cl_Arms',
    'Cl_Temp',
    'F_L_Pin_Vrms',
    'F_L_Pin_Arms',
    'F_L_Pin_Temp',
    'F_L_Pin_Apk',
    'F_L_Pin_Crst',
    'F_R_Pin_Vrms',
    'F_R_Pin_Arms',
    'F_R_Pin_Temp',
    'F_R_Pin_Apk',
    'F_R_Pin_Crst',
    'R_L_Pin_Vrms',
    'R_L_Pin_Arms',
    'R_L_Pin_Temp',
    'R_L_Pin_Apk',
    'R_L_Pin_Crst',
    'R_R_Pin_Vrms',
    'R_R_Pin_Arms',
    'R_R_Pin_Temp',
    'R_R_Pin_Apk',
    'R_R_Pin_Crst',
    'F_L_B_Vrms',
    'F_L_B_Arms',
    'F_L_B_Temp',
    'F_R_B_Vrms',
    'F_R_B_Arms',
    'F_R_B_Temp',
    'R_L_B_Vrms',
    'R_L_B_Arms',
    'R_L_B_Temp',
    'R_R_B_Vrms',
    'R_R_B_Arms',
    'R_R_B_Temp',
    'R_R_B_Vrms',
    'E_R_Pre_psi',
    'P_R_Pressure_psi',
    'S_R_Pre_psi',
    'Man_Br_Pre_psi',
    'Man_Cl_Pre_psi',
    'Man_Exp_Pre_psi',
    'Man_Ret_Pre_psi',
    'Hydraulic_Unit_Pressure_psi',
    'Hydraulic_Unit_Oil_Level',
    'Press_angle',
    'Date'
    ]

batch2 = [
    'F_L_Gibbs_Vrms',
    'F_L_Gibbs_Arms',
    'F_L_Gibbs_Temp',
    'F_R_Gibbs_Vrms',
    'F_R_Gibbs_Arms',
    'F_R_Gibbs_Temp',
    'R_L_Gibbs_Vrms',
    'R_L_Gibbs_Arms',
    'R_L_Gibbs_Temp',
    'R_R_Gibbs_Vrms',
    'R_R_Gibbs_Arms',
    'R_R_Gibbs_Temp',
    'Lub_Motor_Vrms',
    'Lub_Motor_Arms',
    'Lub_Motor_Temp',
    'Lub_Pump_Vrms',
    'Lub_Pump_Arms',
    'Lubrication_Pump_Temp',
    'Lubrication_Oil_Level_mm',
    'Pit_level_mm',
    'E_Motor_Vrms',
    'E_Motor_Arms',
    'E_Motor_Temp',
    'F_Motor_Vrms',
    'F_Motor_Arms',
    'F_Motor_Temp',
    'Man_Fwd_Pre_psi',
    'Man_Rev_Pre_psi',
    'Hydraulic_Motor_Vrms',
    'Hydraulic_Motor_Arms',
    'Hydraulic_Motor_Temp',
    'Hydraulic_Pump_Vrms',
    'Hydraulic_Pump_Arms',
    'Hydraulic_Pump_Temp',
    'P_R_Motor_Vrms',
    'P_R_Motor_Arms',
    'P_R_Motor_Temp',
    'P_R_Motor_Apeak',
    'P_R_GB_Vrms',
    'P_R_GB_Arms',
    'P_R_GB_Temp',
    'P_R_GB_Apeak',
    'Press_angle',
    'Cr_LBlock1_Pre',
    'Date'
    ]

batch1collection = pressdb['BATCH_1']
batch2collection = pressdb['BATCH_2']

#=====================================================================================================================================================
#Column list for constant data check

column_list = [
    'Cl_Vrms',
    'Cl_Arms',
    'M_M_Vrms',
    'M_M_Arms',
    #'Pit_Level_mm', constant 0 value unless flooding occurs in the basement
    'Ram_Adjust_Vrms',
    'Ram_Adjust_Arms',
    'F_L_Pin_Vrms',
    'F_R_Pin_Vrms',
    'R_L_Pin_Vrms',
    'R_R_Pin_Vrms',
    'F_L_Pin_Arms',
    'F_R_Pin_Arms',
    'R_L_Pin_Arms',
    'R_R_Pin_Arms',
    'F_L_Pin_Apk',
    'F_R_Pin_Apk',
    'R_L_Pin_Apk',
    'R_R_Pin_Apk',
    'F_L_Pin_Crst',
    'F_R_Pin_Crst',
    'R_L_Pin_Crst',
    'R_R_Pin_Crst',
    'F_L_B_Vrms',
    'F_R_B_Vrms',
    'R_L_B_Vrms',
    'R_R_B_Vrms',
    'F_L_B_Arms',
    'F_R_B_Arms',
    'R_L_B_Arms',
    'R_R_B_Arms',
    'F_L_Nut_Vrms',
    'F_R_Nut_Vrms',
    'R_L_Nut_Vrms',
    'R_R_Nut_Vrms',
    'F_L_Nut_Arms',
    'F_R_Nut_Arms',
    'R_L_Nut_Arms',
    'R_R_Nut_Arms',
    'Cl_Br_Pre_psi',
    'CBal_Pre_psi',
    #'Lubrication_B_05_Pressure_psi', inconsistent data, values drop to a constant 0 regularly
    'Lub_Motor_Vrms',
    'Lub_Motor_Arms',
    'Lub_Pump_Vrms',
    'Lub_Pump_Arms',
    'Lubrication_Oil_Level_mm',
    'Cr_LBlock1_Pre',
    'F_L_Gibbs_Vrms',
    'F_R_Gibbs_Vrms',
    'R_L_Gibbs_Vrms',
    'R_R_Gibbs_Vrms',
    'F_L_Gibbs_Arms',
    'F_R_Gibbs_Arms',
    'R_L_Gibbs_Arms',
    'R_R_Gibbs_Arms',
    'Main_Air_Pre_psi',
    #'Man_Br_Pre_psi', Mandrels give constant values until die is changed
    #'Man_Cl_Pre_psi',
    #'Man_Exp_Pre_psi',
    #'Man_Ret_Pre_psi',
    #'Man_Fwd_Pre_psi',
    #'Man_Rev_Pre_psi',
    'P_R_Motor_Vrms',
    'P_R_Motor_Arms',
    'Hydraulic_Motor_Vrms',
    'Hydraulic_Motor_Arms',
    'Hydraulic_Pump_Vrms',
    'Hydraulic_Pump_Arms',
    'E_R_Pre_psi',
    #'P_R_Pressure_psi', open issue, sensor giving 0 values
    #'S_R_Pre_psi', 0 values
    'Hydraulic_Unit_Pressure_psi',
    'Hydraulic_Unit_Oil_Level',
    'E_Motor_Vrms',
    'E_Motor_Arms',
    'F_Motor_Vrms',
    'F_Motor_Arms',
    ''
    ]

batch_info_dict = {}

for column in column_list:
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

#======================================================================================================================
#While loop to check for errors

def p11constantdata():

    #Constant Data Check

        end = datetime.now()
        start = end - timedelta(hours = 2)
        #start = datetime(2023,7,5,20,0,0)
        #end = datetime(2023,7,5,22,0,0)

        errorlist = []

        #Query batch 1 data

        QUERY = {'Date' : {'$gte' : start, '$lte' : end}}
        projection = {'_id': 0, 'Press_angle': 1, 'Date': 1, **{batch_info_dict[1][x]: 1 for x in range(len(batch_info_dict[1]))}}
        collection = pressdb['BATCH_1']
        results = collection.find(QUERY, projection)
        df = pd.DataFrame(results).set_index('Date')
        
        #filter batch 1 data

        df['angle_diff'] = df['Press_angle'].diff()

        significant_changes = df['angle_diff'].abs() > 0.001

        filtered_df = df[significant_changes]
        filtered_df = filtered_df.drop(["angle_diff", 'Press_angle'], axis = 1)

        #check for 0 change in sensor data

        if filtered_df.empty:
            print('Press not running')
        else:
            filtered_df_diff = filtered_df.diff().abs()
            sum_diff = filtered_df_diff.sum(axis=0)
            for x in batch_info_dict[1]:
                if sum_diff[x] == 0:
                    errorlist += [x]

        #Query batch 2 data

        QUERY = {'Date' : {'$gte' : start, '$lte' : end}}
        projection = {'_id': 0, 'Press_angle': 1, 'Date': 1, **{batch_info_dict[2][x]: 1 for x in range(len(batch_info_dict[2]))}}
        collection = pressdb['BATCH_2']
        results = collection.find(QUERY, projection)
        df2 = pd.DataFrame(results).set_index('Date')
        
        #filter batch 2 data

        df2['angle_diff'] = df2['Press_angle'].diff()

        significant_changes2 = df2['angle_diff'].abs() > 0.001

        filtered_df2 = df2[significant_changes2]
        filtered_df2 = filtered_df2.drop(["angle_diff", 'Press_angle'], axis = 1)

        #check for 0 change in sensor data

        if filtered_df2.empty:
            print('Press not running')
        else:
            filtered_df_diff2 = filtered_df2.diff().abs()
            sum_diff2 = filtered_df_diff2.sum(axis=0)
            for x in batch_info_dict[2]:
                if sum_diff2[x] == 0:
                    errorlist += [x]

        #checks for any sensor constant data and if there is, sends an email with list of sensors
        
        if len(errorlist) == 0:
            print('No constant data errors')
        else:
            print(errorlist)
            print("These sensors have constant data")
            print()

#===================================================================================================================
        nan_list = []

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
        