import pandas as pd
from datetime import datetime
from datetime import timedelta
import pymongo
import matplotlib.pyplot as plt
import time
import os

myclient_global = pymongo.MongoClient(host = '128.121.34.13', Connect = True)
batch_info_dict={}
column_list=[]

press="Press_21"
press_db = myclient_global[press]

if press == "Press_11" or press == "Press_21" or press=="Press_05":
    batches = [1,2]
elif press == "Press_24":
    batches = [1,2,3,4]
#=====================================================================================
#Get all Sensors in a list
for batch in batches:
    collection = press_db["BATCH_" + str(batch)]
    document = collection.find().limit(1).sort("_id", -1)
    for field in [document[0].keys()]:
        column_list += field
#Turn column_list into set and back into list to avoid duplicate values and removing date and id and other unneccesary columns
column_list = set(column_list)
column_list = list(column_list)
column_list.remove("Date")
column_list.remove("_id")
if press == "Press_11":
    column_list.remove("Ram_Adjust_Chain_On/Off")
#=======================================================================================
#Sorting sensors into their batch
for column in column_list:
    for batch in batches:
        collection = "BATCH_"+str(batch)
        db = press_db[collection]
        results = db.find().limit(1).sort("_id",-1)
        recent_doc = results[0]
        if column in recent_doc.keys():
            if batch not in batch_info_dict:
                batch_info_dict[batch] = [column]
            else:
                batch_info_dict[batch].append(column)
#===================================================================
#sorting lists in batch info dict so it is easier to plot on excel
for batch in batch_info_dict:
    batch_info_dict[batch].sort()


def get_threshold(sensor, startdatestr, enddatestr, pressangle, batch_info_dict, press_db):
    startdatelist = startdatestr.split(',')
    enddatelist = enddatestr.split(',')

    #turning all str in list to int
    for i in range(0, len(startdatelist)):
        startdatelist[i] = int(startdatelist[i])
    for i in range(0, len(enddatelist)):
        enddatelist[i] = int(enddatelist[i])

    startdate = datetime(startdatelist[0],startdatelist[1],startdatelist[2],startdatelist[3],startdatelist[4],startdatelist[5])
    enddate = datetime(enddatelist[0],enddatelist[1],enddatelist[2],enddatelist[3],enddatelist[4],enddatelist[5])

    for key in batch_info_dict:
        if sensor in batch_info_dict[key]:
            sbatch = "BATCH_" + str(key)
    
    if press=="Press_11":
        pressangle="Press_angle"
    elif press=="Press_24":
        pressangle=="Press_Angle"
    else:
        pressangle=""

    collection = press_db[sbatch]
    if pressangle == "":
        projection = {'_id':0, 'Date':1, sensor:1}
    else:
        projection = {'_id':0, 'Date':1, pressangle : 1, sensor:1}
    #====================================================================================
    #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
    if pressangle == "":
        QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}}]}
    else:
        QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':0, '$lte': 360}}, {sensor : {'$lte' : 10000}}]}
    results = collection.find(QUERY, projection)
    df = pd.DataFrame(results).set_index("Date")
    #====================
    return (df[sensor].rolling(window = timedelta(minutes=1)).mean()).mean().round(2)
    
    

def check_movingavg_threshold(startdate,enddate ,batch_info_dict, press_db):
    
    #depending on requirement
    train_startdate=startdate-timedelta(hours=7)
    train_enddate=startdate-timedelta(hours=1)    
    
    # train_startdate=datetime(2023,9,17,23,45,0)
    # train_enddate=datetime(2023,9,18,12,45,0)

    train_startdate=train_startdate.strftime("%Y,%m,%d,%H,%M,%S")
    train_enddate=train_enddate.strftime("%Y,%m,%d,%H,%M,%S")

    #current_dir=os.getcwd()
    #******Replace with personal local path *********
    sensor_sheet=pd.read_excel(r"C:\Users\JSharma\Downloads\AllPressAnalysis (2)\AllPressAnalysis\live_sensors_list.xlsx", sheet_name= press,usecols="E")
    lst=sensor_sheet.values.tolist()
    lst_temp=[]
    for i in lst:
        for j in i:
            if "Temp" in j:
                lst_temp.append(j)
    alert_dict={}
    for sensor in lst_temp:
        alert_dict[sensor]=[]

    for sensor in lst_temp:
        # print("******************************************************")
        # print()
        for key in batch_info_dict:
            if sensor in batch_info_dict[key]:
                sbatch = "BATCH_" + str(key)

        if press=="Press_11":
            pressangle="Press_angle"
        elif press=="Press_24":
            pressangle=="Press_Angle"
        else:
            pressangle=""

        collection = press_db[sbatch]

        if pressangle == "":
            projection = {'_id':0, 'Date':1, sensor:1}
        else:
            projection = {'_id':0, 'Date':1, pressangle : 1, sensor:1}
        #====================================================================================
        #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
        if pressangle == "":
            QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}}]}
        else:
            QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}},{pressangle: {'$gte':0,'$lte':360}}]}
        results = collection.find(QUERY, projection)
        df = pd.DataFrame(results).set_index("Date")
        threshold=get_threshold(sensor,train_startdate, train_enddate,pressangle, batch_info_dict, press_db)
        
        #====================
        trial_df=df.groupby(pd.Grouper(freq="T")).mean()
        df=trial_df[sensor].rolling(window = timedelta(minutes=1)).mean().round(2)
        try:
            for index,value in df.items():
                if value >= threshold+3:
                    # print("Alert for sensor: ", sensor)
                    # print()
                    # print("Time of alert: ",index)
                    # print()
                    # print("High Value: ",value)
                    # print("Ideal value: ", threshold)
                    # print()
                    if value not in alert_dict[sensor]:
                        alert_dict[sensor].append(value)
                        #time.sleep(1)
            #print(sensor,": ",threshold)
            #print()
        except:
            print("No data for: ",sensor)
            continue
    print(alert_dict)
    print()




def get_threshold_raw(sensor, startdate, enddate, batch_info_dict, press_db):
    for key in batch_info_dict:
        if sensor in batch_info_dict[key]:
            sbatch = "BATCH_" + str(key)
            
    if press=="Press_11":
        pressangle="Press_angle"
    elif press=="Press_24":
        pressangle=="Press_Angle"
    else:
        pressangle=""
    
    collection = press_db[sbatch]
    projection = {'_id':0, 'Date':1, sensor:1}
    #====================================================================================
    #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
    if pressangle == "":
        QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}}]}
    else:
        QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':0, '$lte': 360}}, {sensor : {'$lte' : 10000}}]}
    results = collection.find(QUERY,projection)
    df = pd.DataFrame(results).set_index("Date")
    return df.mean().iloc[0].round(2)
    

def check_threshold_raw(startdate, enddate, batch_info_dict, press_db):

    #******Replace with personal local path *********
    sensor_sheet=pd.read_excel(r"C:\Users\ikurmude\Downloads\AllPressAnalysis\AllPressAnalysis\live_sensors_list.xlsx", sheet_name= press,usecols="E")
    lst=sensor_sheet.values.tolist()
    lst_temp=[]
    for i in lst:
        for j in i:
            if "Temp" in j:
                lst_temp.append(j)
    alert_dict={}
    for sensor in lst_temp:
        alert_dict[sensor]=[]
    for sensor in lst_temp:
        # print("***************************************************")
        # print()
        for key in batch_info_dict:
            if sensor in batch_info_dict[key]:
                sbatch = "BATCH_" + str(key)
                
        if press=="Press_11":
            pressangle="Press_angle"
        elif press=="Press_24":
            pressangle=="Press_Angle"
        else:
            pressangle=""
        
        collection = press_db[sbatch]
        projection = {'_id':0, 'Date':1, sensor:1}
        #====================================================================================
        #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
        if pressangle == "":
            QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}}]}
        else:
            QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':0, '$lte': 360}}, {sensor : {'$lte' : 10000}}]}
        results = collection.find(QUERY,projection)
        df = pd.DataFrame(results).set_index("Date")

        threshold=get_threshold_raw(sensor,startdate-timedelta(hours=7),startdate-timedelta(hours=1),batch_info_dict,press_db)

        comp_df=df.groupby(pd.Grouper(freq="T")).mean()
        comp_df=comp_df[sensor].squeeze()
        #print(comp_df.head())
        try:
            for index,value in comp_df.items():
                if round(value,2) >= threshold+3:
                    # print("Alert for sensor: ", sensor)
                    # print()
                    # print("Time of alert: ",index)
                    # print()
                    # print("High Value: ",value)
                    # print("Ideal value: ", threshold)
                    # print()
                    if value not in alert_dict[sensor]:
                        alert_dict[sensor].append(round(value,2))
                    #time.sleep(1)
            #print(sensor,": ",threshold)
        except:
            print("No data collected: ",sensor)
            continue
            
    print(alert_dict)
       

#check value
check_movingavg_threshold(datetime(2023,9,18,13,45,0),datetime(2023,9,19,23,45,0),batch_info_dict,press_db)
# check_threshold_raw(datetime(2023,9,18,13,45,0),datetime(2023,9,19,23,45,0),batch_info_dict,press_db)