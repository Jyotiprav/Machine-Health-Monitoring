import json
import paho.mqtt.client as mqtt
import pymongo
import pandas as pd
import os
from datetime import datetime, time, timedelta
import subprocess
import sys
import matplotlib.pyplot as plt
from datetimerange import DateTimeRange
from scipy.signal import find_peaks
import threading
import platform
import logging

time_now = datetime.now()
logging.basicConfig(level=logging.INFO,
                    #filename=f"send_signal_to_plc_Beckhoff_{time_now.date()}.log",
                    #filemode="w",
                    format="%(process)d-%(asctime)s - %(levelname)s-%(message)s",
                    datefmt="%d-%b-%y %H:%M:%S")


##############################################
######### Alert return values for PLC ########
##############################################
critical_alert_code             = 2
warning_code                    = 1


##############################################
##### All sheets needed declared globally ####
##############################################
threshold_sheet = "press_threshold_list.xlsx"
live_sensor_sheet = "live_sensors_list.xlsx"
excel_file_with_ips = "IP_list.xlsx"
sensor_threshold_percent={"lub": 30,"vrms": 50, "arms": 50, "apk": 50, "crst": 50}   #change percent threshold according to need, update in moving avg function


ip_data = pd.ExcelFile(excel_file_with_ips)
presses_and_IP = {} # Pass the list of IPs as parmeters from main. Make it read the IPS from excel or csv file. #done
#code that parses through the IP_list excel file to get all the presses and their corresponding IPs
df = ip_data.parse(index_col='Press #') 
dict = df.to_dict()['Broker IP']
for num in dict:
    if int(num) < 10:
        presses_and_IP[f"0{num}"] = dict[num]

    else:
        presses_and_IP[f"{num}"] = dict[num]


##############################################
######### PLC Steup ##########################
##############################################
port = 1883
keep_alive = 60
Press_24_topic = 'Press_24/#'
Press_21_topic = 'Press_21/#'
Press_11_topic = 'Press_11/#'
Press_05_topic = 'Press_05/#'

mqttc_24 = mqtt.Client(client_id="Jetson alert_check_client_p24_test", clean_session=False)
mqttc_21_and_05 = mqtt.Client(client_id="Jetson alert_check_client_p21_and_p05_test", clean_session=False)
mqttc_11 = mqtt.Client(client_id="Jetson alert_check_client_p11_test", clean_session=False)

# Connect to database and send the alerts directly to database

db_IP_address = "128.121.34.13"
myclient_global = pymongo.MongoClient(db_IP_address) 
alert_db = myclient_global["Press_Alerts"]


#helper variables
previous_alert = None
# press_chosen = ''


# Connect to MQTT broker
def on_connect_24(client, userdata, flags, rc):
    if ( rc == 0 ):
        print("Connected to Mqtt Broker successfully")
        mqttc_24.subscribe(Press_24_topic,0)
        # client.subscribe([(Press_24_topic, 0), ("another/topic", 0)])
    else:                   
        print("Connect returned result code: " + str(rc))

def on_connect_21_and_05(client, userdata, flags, rc):
    if ( rc == 0 ):
        print("Connected to Mqtt Broker successfully")
        # client.subscribe(Press_24_topic,0)
        mqttc_21_and_05.subscribe([(Press_21_topic, 0), (Press_05_topic, 0)])
    else:                   
        print("Connect returned result code: " + str(rc))

def on_connect_11(client, userdata, flags, rc):
    if ( rc == 0 ):
        print("Connected to Mqtt Broker successfully")
        mqttc_11.subscribe(Press_11_topic,0)
        # client.subscribe([(Press_24_topic, 0), ("another/topic", 0)])
    else:                   
        print("Connect returned result code: " + str(rc))

def on_publish(client,userdata,results):    
    # what do you want to happen when something is published
    pass

def on_message(client, userdata, msg):
    # what do you want to happen when a message is sent (basically always)
    pass
#^ =========================================================================================================================================


##############################################
######### Check all valid Connections  ########
##############################################
def check_PLC_MQTT_DB_connection(press_number,host):
    logging.info(f"check_PLC_MQTT_DB_connection() : {press_number}")
    global Broker_connections
    global Db_connections
    global Overall_connections
    Broker_connections = {} # Will be a dictionary e.g. {'press_number':Boolean}
    Db_connections = {}
    Overall_connections = {}
    
    try: 
        from time import sleep
        sleep(3)     
        param = '-n' if platform.system().lower()=='windows' else '-c'
        # Building the command. Ex: "ping -c 1 google.com"
        command = ['ping', param, '1' '''#How many packets you want to send during the ping''' , host]
        ping_command_output = subprocess.call(command, 
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.STDOUT)
        if ping_command_output != 0:
            logging.error("PLC to Broker Connection Error.")
            Broker_connections[press_number] = False
            logging.warning(f"Check the PLC connection for Press_{press_number} with the IP of {presses_and_IP[press_number]}")
            print()
        else:
            print("--> PLC Connections are good.")

        pressdb = myclient_global[f'Press_{press_number}']        
        first_batch_db = pressdb["BATCH_1"]        
        current_time = datetime.now().replace(second=0, microsecond=0)       
        query = {'Date' : {'$gte' : current_time}}    
        logging.debug(f"Getting {current_time} document from the database...")     
        result = None        
        result = first_batch_db.find_one(query)
        if not result:        
            logging.error("MQTT to DB Connection broken.")
            logging.warning(f"Check the DB connection for Press_{press_number}.")
            Db_connections[press_number] = False
            print()
            for last_update in first_batch_db.find().sort('$natural',-1).limit(1):        
                logging.warning(f"The last doc added for Press_{press_number} was at {last_update['Date']}")
                print()

        else:
            print(f"--> DB Connections for Press_{press_number} and {host} Good!")
            logging.debug(f"All Connections for Press_{press_number} Good!")
            Overall_connections[press_number], Broker_connections[press_number], Db_connections[press_number] = True, True, True
        # print("__________________________________________________________________________________________________________")
    except:
        logging.critical("Network/Database Down!")
        for press in presses_and_IP:
            Broker_connections[press], Db_connections[press] = False, False
    print("--> Config mqtt client ")
    connection_status = list(Overall_connections.values())[0]
    if connection_status:
            try:
                mqttc_11.on_publish = on_publish
                mqttc_11.on_connect = on_connect_11
                mqttc_11.on_message = on_message
                mqttc_11.connect(presses_and_IP[press_number], int(port), int(keep_alive))
                print("--> mqtt client config done")
            except Exception as e:
                logging.error(f"--> ERROR:Can not make mqtt client. Can not connect to Press_{working_press}")
                Broker_connections[f"Press_{press_number}"] = False
    return Overall_connections
    
def get_job(press_number):
    # return "MRT68430744_5AA"
    # if press == '24':
    pressdb = myclient_global[f'Press_{press_number}']
    recent_document_from_db = pressdb["BATCH_1"].find().limit(1).sort("$natural", -1)
    recent_document_from_db = recent_document_from_db[0]
    recent_job = recent_document_from_db['job_id']
    if recent_job == '':
        return "No Job Inputted"
    return recent_job
    # else: return None
    
def check_missing_datapoints(press_number:str):
    pressdb = myclient_global[f'Press_{press_number}']
    if press_number=="24":
        press_angle_tag_name = 'Press_Angle'
        collection_list = ["BATCH_1"]
    elif press_number=="11":
        press_angle_tag_name = 'Press_angle'
        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings

    # collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
    # collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]   #* List of all collection names as strings
    db_keys_list = []
    #until updating
    QUERY = {"Date": {'$gte': time_an_hour_before, '$lt': time_now}}
    for collection in collection_list:
        allkeysbatch1 = pressdb[collection].find(QUERY).limit(1)
        db_keys_list += list(allkeysbatch1[0].keys())
    db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date" and i!="job_id"]#* List of all datapoints in the database
    db_keys_list = set(db_keys_list)
    current_dir=os.getcwd()
    sensor_sheet=pd.read_excel((live_sensor_sheet), sheet_name= f"Press_{press_number}",usecols="E,H")
    if press_number== "24":
        excel_keys_list=list(sensor_sheet["Updated labels"])
    elif press_number=="21":
        excel_keys_list=list(sensor_sheet["Old labels"])
    else:
        excel_keys_list=list(sensor_sheet["Tag Name"])
    miss_list=[]
    if len(db_keys_list)>len(excel_keys_list): 
        for i in db_keys_list:
            if i not in excel_keys_list:
                miss_list.append(i)
                #print(f"Datapoint {i} missing from excel sheet.")
    elif len(db_keys_list)<len(excel_keys_list):
        for i in excel_keys_list:
            if i not in db_keys_list:
                miss_list.append(i)
                #print(f"Datapoint {i} missing from Database.")
    return miss_list

    
def is_press_running(press_number:str):
    pressdb = myclient_global[f'Press_{press_number}']
    collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
    collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
    press_angle_tag_name =""
    if press_number=="24":
        press_angle_tag_name = 'Press_Angle'
        first_collection = "BATCH_1"
    elif press_number=="11":
        press_angle_tag_name = 'Press_angle'
        first_collection = collection_list[0]
    # get all the collection name in database
    end = time_now
    start = time_an_hour_before
    errorlist = []
    QUERY = {"Date": {'$gte': start, '$lt':  end}}
    allkeysbatch1 = pressdb[first_collection].find().sort('_id',-1)
    db_keys_list = list(allkeysbatch1[0].keys())
    db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date" and i != "Ram_Adjust_Chain_On/Off" and "press" not in i and i!="job_id"]#* List of all datapoints in the database
    db_keys_list = set(db_keys_list)
    projection = {'_id': 0, 'Date': 1, **{x: 1 for x in db_keys_list}}
    results = pressdb[first_collection].find(QUERY, projection)
    df = pd.DataFrame(results).set_index("Date")
    df['angle_diff'] = df[press_angle_tag_name].diff()
    significant_changes = df['angle_diff'].abs() > 0.001
    filtered_df = df[significant_changes]
    filtered_df = filtered_df.drop(["angle_diff"], axis = 1)
    if filtered_df.empty:
        return False
    else:
        return True
        


# check if data is constant (PLC fault)
def check_constantdata(press_number:str):
    pressdb = myclient_global[f'Press_{press_number}']
    if press_number=="24":
        press_angle_tag_name = 'Press_Angle'
        collection_list = ["BATCH_1"]
    elif press_number=="11":
        press_angle_tag_name = 'Press_angle'
        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
   
    end = time_now
    start = time_an_hour_before
    errorlist = []
    QUERY = {"Date": {'$gte': start, '$lt':  end}}
    
    for collection in collection_list:
        allkeysbatch1 = pressdb[collection].find().sort('_id',-1)
        db_keys_list = list(allkeysbatch1[0].keys())
        db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date" and i != "Ram_Adjust_Chain_On/Off" and "press" not in i and i!="job_id"]#* List of all datapoints in the database
        db_keys_list = set(db_keys_list)
        projection = {'_id': 0, 'Date': 1, **{x: 1 for x in db_keys_list}}
        results = pressdb[collection].find(QUERY, projection)
        df = pd.DataFrame(results).set_index("Date")
        df_diff = df.diff().abs()
        sum_diff = df_diff.sum(axis=0)
        for x in db_keys_list:
            if sum_diff[x] == 0  and "fdl" not in x:
                errorlist += [x]
    return errorlist


    
    
# check for thresholds and set them. Thresholds are based on the data we got in march
def get_threholds(press_number,component,startdate,enddate):
    # thresholds are based on job run 597_598 in march . 
    # datapoints to check
    if "Pre" in component or "psi" in component:
    #if "psi" in component:
        list_of_datapoints=[component]
    else:
        list_of_datapoints = [component+"_Vrms",component+"_Arms",component+"_Apk",component+"_Crst",component+"_Crest",component+"_Apeak",component+"_Pre",
                              component+"_vrms",component+"_arms",component+"_apk",component+"_crst",component+"_psi"]
    pressdb = myclient_global[f'Press_{press_number}']
    # Get component's batch/collection
    collection_list = pressdb.list_collection_names() #List of all collection names as strings
    collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
    list_of_sensor_in_collection = []
    threshold_dict ={}
    # QUERY1 = {"Date": {'$gte': datetime(2023,5,23,10,0,0), '$lt':  datetime(2023,5,23,10,1,0)}}
    for collection in collection_list:
        allkeysbatch1 = pressdb[collection].find().sort('_id',-1)
        db_keys_list=list(allkeysbatch1[0].keys())
        list_of_sensor_in_collection = [sensor for sensor in list_of_datapoints if sensor in db_keys_list]
        if list_of_sensor_in_collection !=[]:
            for sensor in list_of_sensor_in_collection:
                if "Vrms" in sensor or "vrms" in sensor:
                #if "vrms" in sensor:
                        QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3}}]}
                elif "Arms" in sensor or "Apk" in sensor or "Apeak" in sensor or "Crst" in sensor or "arms" in sensor or "apk" in sensor or "crst" in sensor:
                #elif "arms" in sensor or "apk" in sensor  or "crst" in sensor
                        QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3000}}]}
                else:
                        QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}}]}
                try:
                    if press_number == "24":
                        projection = {'_id':0, 'Date':1, "Press_Angle":1, sensor:1}
                        results = pressdb[collection].find(QUERY, projection)      
                        df = pd.DataFrame(results).set_index("Date")
                        # print(len(df))
                        df = df.iloc[::3]
                        # print(len(df))
                        #Filter the downtime
                        df['angle_diff'] = df["Press_Angle"].diff()
                        significant_changes = df['angle_diff'].abs() > 0.001
                        filtered_df = df[significant_changes]
                        filtered_df = filtered_df.drop(["angle_diff", "Press_Angle"], axis = 1)
                        if ( collection=="BATCH_4") and startdate> datetime(2023,3,31) and startdate<=datetime(2023,10,4):
                            current_dir=os.getcwd()
                            sensorsheet=pd.read_excel((live_sensor_sheet), sheet_name= "Press_24", usecols="E,H")
                            old_lst=sensorsheet["Old labels"].tolist()
                            new_lst=sensorsheet["Updated labels"].tolist()
                            if sensor in old_lst:
                                new_sensor=new_lst[old_lst.index(sensor)]
                                threshold_dict.update({new_sensor : (filtered_df[sensor].rolling(window = timedelta(seconds=15),center = True).mean()).mean()})
                        else:
                            threshold_dict.update({sensor : (filtered_df[sensor].rolling(window = timedelta(seconds=15),center = True).mean()).mean()})
                    elif press_number =="11":
                        projection = {'_id':0, 'Date':1, "Press_angle":1, sensor:1}
                        results = pressdb[collection].find(QUERY, projection)      
                        df = pd.DataFrame(results).set_index("Date")
                        
                        #Filter the downtime
                        df['angle_diff'] = df["Press_angle"].diff()
                        significant_changes = df['angle_diff'].abs() > 0.001
                        filtered_df = df[significant_changes]
                        filtered_df = filtered_df.drop(["angle_diff", "Press_angle"], axis = 1)
                        threshold_dict.update({sensor : (filtered_df[sensor].rolling(window = timedelta(seconds=15),center = True).mean()).mean()}) 
                    else:                                                                                                                     # No filtering based on press angle for press 21
                        projection = {'_id':0, 'Date':1, sensor:1}
                        results = pressdb[collection].find(QUERY, projection) 
                        # document_count = pressdb[collection].count_documents(QUERY)    
                        df = pd.DataFrame(results).set_index("Date")
                        #Filter the downtime
                        threshold_dict.update({sensor : (df[sensor].rolling(window = timedelta(seconds=15),center = True).mean()).mean()})
                except Exception as e:
                    continue
    return threshold_dict

def get_previous_thresholds(press_number,STATIC_THRESHOLD_DICT = {}):
    pressdb = myclient_global[f'Press_{press_number}']
    db_keys_list = []
    #1.  Read the reference threshold timings from excel
    try:
        thresholds = pd.read_excel(threshold_sheet,f"Press {press_number}").set_index("Jobs").to_dict()
    except Exception as e:
        print(e)
    
    #2. Get the current running JOB
    if press_number == "11":
        current_job = "MRT-68430748/49" # Default high tonage job because we don't have job ID.
        start_and_end = [thresholds["Start date"][current_job], 
                         thresholds["End date"][current_job]]
        
    elif press_number == "24":
        first_batch_db = pressdb['BATCH_1']
        for doc in first_batch_db.find().sort('$natural',-1).limit(1):
            current_job = doc['job_id']
            start_and_end = [thresholds["Start date"][current_job], thresholds["End date"][current_job]]
    for date in start_and_end:
        #splitting the date
        runtime = date.split(',')
        # #changing it all from str to int to be put into datetime format
        for i in range(0, len(runtime)):
            runtime[i] = int(runtime[i])
        if date == start_and_end[0]:
            job_start_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
        elif date == start_and_end[1]:
            job_end_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
                
    #3. Get the datapoints name from database
    comp_lst=[]
    collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
    collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
    for collection in collection_list:
        allkeysbatch1=pressdb[collection].find().sort('_id',-1)
        db_keys_list += list(allkeysbatch1[0].keys())
        db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date" and i!="Ram_Adjust_Chain_On/Off" and i!="job_id" and i not in const_data_lst]#* List of all datapoints in the database
    db_keys_list = set(db_keys_list)

    #4. Making com_list . Don't know why??
    for i in db_keys_list:
        if "Vrms" in i or "Arms" in i or"Crest" in i or "Apk" in i or "Crst" in i or "Apeak" in i or "vrms" or "arms" or  "crst" or "apk" in i:
            code_list=i.split("_")
            comp="_".join(code_list[:len(code_list)-1])
            if comp not in comp_lst:
                comp_lst.append(comp)
        else:
            comp_lst.append(i)
    mov_avg_dict={}

    #5. Loop to generate the thresholds
    for component in comp_lst:
        if press_number == "11":
            threshold_dict = get_threholds(press_number,component,job_start_date,job_end_date)
            print(threshold_dict)
                                        

# get_thresholds_based on job
def get_threshold_per_job(press_number):
    job_id = get_job(press_number)
    pressdb = myclient_global[f'Press_{press_number}']
    logging.info("--> check_component_moving_average : Reading excel file")
    try:
        thresholds = pd.read_excel(threshold_sheet,f"Press {press_number}").set_index("Jobs").to_dict()
    except:
        logging.info("check_component_moving_average : Can not read the thresholds.xlxs")
    
def get_press_datapoints(press_number):
    all_datapoint_list = []
    component_list=[]
    
    pressdb         = myclient_global[f'Press_{press_number}'] # Get press database
    collection_list = pressdb.list_collection_names()  # List of all collection names as strings
    collection_list =[x for x in collection_list if "BATCH" in x and "COPY" not in x]  # List of all collection names as strings
    const_data_lst=check_constantdata(press_number) # Get the points that have constant data. No need to calculate average of constant data
    for collection in collection_list:
        allkeysbatch1=pressdb[collection].find().sort('_id',-1) # Getting current keys
        all_datapoint_list += list(allkeysbatch1[0].keys())
    all_datapoint_list  = [i for i in all_datapoint_list 
                           if i!="_id"  #Avoid id key
                           and i!="Date" #Avoid date key
                           and i!="Ram_Adjust_Chain_On/Off" #Avoid boolean values
                           and i!="job_id" # avoid job string values
                           and i not in const_data_lst # avoid all constant points
                           ]
    all_datapoint_list = set(db_keys_list) # Remove duplicates
    print("Length of datapoints," ,len(all_datapoint_list))
    for i in all_datapoint_list:
        # if i.split("_")[-1] in ()
        if "Vrms" in i or "Arms" in i or"Crest" in i or "Apk" in i or "Crst" in i or "Apeak" in i or "vrms" or "arms" or  "crst" or "apk" in i: # If any of them are in datapoint
            code_list=i.split("_")
            comp="_".join(code_list[:len(code_list)-1])
            if comp not in comp_lst:
                comp_lst.append(comp)
        else:
            comp_lst.append(i)
    mov_avg_dict={}
    print("Component found: ",len(comp_lst))
    
    
    

# component moving average check
def check_component_moving_average(press_number):
    logging.info("--> check_component_moving_average : Executing....")
    global time_an_hour_before
    global time_now
    startdate =time_an_hour_before
    enddate = time_now
    pressdb = myclient_global[f'Press_{press_number}']
    logging.info("--> check_component_moving_average : Reading excel file")
    try:
        thresholds = pd.read_excel(threshold_sheet,f"Press {press_number}").set_index("Jobs").to_dict()
    except:
        logging.info("check_component_moving_average : Can not read the thresholds.xlsx")
    # get all the collection name in database
    db_keys_list = []
    comp_lst=[]
    collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
    collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
    QUERY = {"Date": {'$gte': datetime(2023,5,23,10,0,0), '$lt':  datetime(2023,5,23,10,1,0)}}
    const_data_lst=check_constantdata(press_number)
    for collection in collection_list:
        allkeysbatch1=pressdb[collection].find().sort('_id',-1) # Getting current keys
        db_keys_list += list(allkeysbatch1[0].keys())
    db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date" and i!="Ram_Adjust_Chain_On/Off" and i!="job_id" and i not in const_data_lst]#* List of all datapoints in the database
    db_keys_list = set(db_keys_list)
    print("Length of datapoints," ,len(db_keys_list))
    #=============================================================
    for i in db_keys_list:
        if "Vrms" in i or "Arms" in i or"Crest" in i or "Apk" in i or "Crst" in i or "Apeak" in i or "vrms" or "arms" or  "crst" or "apk" in i:
            code_list=i.split("_")
            comp="_".join(code_list[:len(code_list)-1])
            if comp not in comp_lst:
                comp_lst.append(comp)
        else:
            comp_lst.append(i)
    mov_avg_dict={}
    print("Component found: ",len(comp_lst))
    for component in comp_lst:
        try:
            if press_number == "21":
                #threshold_dict = get_threholds(press_number,component,datetime(2023,5,2,12,45,0),datetime(2023,5,3,4,0,0))
                threshold_dict = get_threholds(press_number,component,datetime(2023,8,16,22,0,0),datetime(2023,8,17,10,45,0))  #732/33
                recent_values = get_threholds(press_number,component,startdate,enddate)
                for i in threshold_dict:
                    if (threshold_dict[i])!=0:
                         if abs(((recent_values[i]-threshold_dict[i])/(threshold_dict[i])))*100> 50:
                             mov_avg_dict[i]={"Rate of change":round(((recent_values[i]-threshold_dict[i])/(threshold_dict[i]))*100),"Threshold mean value":round(threshold_dict[i],3), "Recent mean values":round(recent_values[i],3)}
                             
            elif press_number == "24":
                logging.info(component)
                job_start = None
                job_end = None
                list_of_all_jobs    = [i["Job Number"] for i in (pressdb["Job_details"].find())] 
                first_batch_db = pressdb['BATCH_1']
                current_job = get_job(press_number)
                # for doc in first_batch_db.find().sort('$natural',-1).limit(1):
                #     current_job = doc['job_id']
                start_and_end = [thresholds["Start date"][current_job], thresholds["End date"][current_job]]
                for date in start_and_end:
                    #splitting the date
                    runtime = date.split(',')
                    # #changing it all from str to int to be put into datetime format
                    for i in range(0, len(runtime)):
                        runtime[i] = int(runtime[i])
                    if date == start_and_end[0]:
                        job_start_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
                    elif date == start_and_end[1]:
                        job_end_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
                logging.info(f"Thresholds data for {current_job} {job_start_date} {job_end_date}")
                if component =="hyd_unitMain":
                    threshold_dict = get_threholds(press_number,component,job_start_date,job_end_date)
                    QUERY={"Date":{"$gte": startdate, "$lt": enddate}}
                    collection=pressdb["BATCH_2"].find(QUERY)
                    df= pd.DataFrame(collection)
                    inp=df["hyd_unitMain_psi"].to_numpy()
                    peaks,dict_peaks=find_peaks(-inp, height=[-500,-100])
                    if len(peaks)!=0:
                        recent_values=sum(dict_peaks["peak_heights"])/len(dict_peaks["peak_heights"])
                        if abs(((abs(recent_values)-threshold_dict["hyd_unitMain_psi"])/(threshold_dict["hyd_unitMain_psi"])))*100> percent_change:
                            mov_avg_dict[component]={"Rate of change":round(((abs(recent_values)-threshold_dict["hyd_unitMain_psi"])/(threshold_dict["hyd_unitMain_psi"]))*100),"Threshold mean value":round(threshold_dict["hyd_unitMain_psi"],3), "Recent mean values":round(abs(recent_values),3)}
               
                else:
                        # threshold_dict = get_threholds(press_number,component,datetime(2023,4,13,1,0,0),datetime(2023,4,13,12,45,0)) #job 7817 because it was running on oct 3
                        threshold_dict  = get_threholds(press_number,component,job_start_date,job_end_date)
                        logging.info(threshold_dict)
                        recent_values   = get_threholds(press_number,component,startdate,enddate)
                        logging.info(recent_values)
                        for i in threshold_dict:
                            if (threshold_dict[i])!=0:
                                if "lub" in i or "Lube" in i or "Lub" in i:                #last 2 conditions only until batch updation complete for p21 and p11
                                    percent_change=sensor_threshold_percent["lub"]       # can customize percent threshold according to data type
                                else:
                                    percent_change=sensor_threshold_percent["vrms"]
                                if abs(((recent_values[i]-threshold_dict[i])/(threshold_dict[i])))*100> percent_change:
                                    mov_avg_dict[i]={"Rate of change":round(((recent_values[i]-threshold_dict[i])/(threshold_dict[i]))*100),"Threshold mean value":round(threshold_dict[i],3), "Recent mean values":round(recent_values[i],3)}
            elif press_number == "11":
                logging.info(component)
                current_job = get_job(press_number)
                start_and_end = [thresholds["Start date"][current_job], thresholds["End date"][current_job]]
                for date in start_and_end:
                    #splitting the date
                    runtime = date.split(',')
                    # #changing it all from str to int to be put into datetime format
                    for i in range(0, len(runtime)):
                        runtime[i] = int(runtime[i])
                    if date == start_and_end[0]:
                        job_start_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
                    elif date == start_and_end[1]:
                        job_end_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
                logging.info(f"Thresholds data for {current_job} {job_start_date} {job_end_date}")
                threshold_dict  = get_threholds(press_number,component,job_start_date,job_end_date)
                logging.info(threshold_dict)
                recent_values   = get_threholds(press_number,component,startdate,enddate)
                logging.info(recent_values)
                print("="*100)
                for i in threshold_dict:
                    if (threshold_dict[i])!=0:
                            if abs(((recent_values[i]-threshold_dict[i])/(threshold_dict[i])))*100> 50:
                                mov_avg_dict[i]={"Rate of change":round(((recent_values[i]-threshold_dict[i])/(threshold_dict[i]))*100),"Threshold mean value":round(threshold_dict[i],3), "Recent mean values":round(recent_values[i],3)}
            elif press_number == "05":
                threshold_dict = get_threholds(press_number,component,datetime(2023,9,28,0,0,0),datetime(2023,9,28,10,0,0))   #temporary reference job for press05
                recent_values = get_threholds(press_number,component,startdate,enddate)
                for i in threshold_dict:
                    if (threshold_dict[i])!=0:
                        if abs(((recent_values[i]-threshold_dict[i])/(threshold_dict[i])))*100> 50:
                             mov_avg_dict[i]={"Rate of change":round(((recent_values[i]-threshold_dict[i])/(threshold_dict[i]))*100),"Threshold mean value":round(threshold_dict[i],3), "Recent mean values":round(recent_values[i],3)}

        except:
            continue

    #checking pressure change for child lubrication with post filter sensor

    if press_number=="24":
        if "lub_postFltr_psi" not in mov_avg_dict.keys():
            for sensor in mov_avg_dict.keys():
                if "lub" in sensor and "post" not in sensor and "pre" not in sensor:
                    mov_avg_dict[sensor]["Alert"]="No change in post filter pressure"
    elif press_number=="21":
        if "Prs_LubePostFilterPre" not in mov_avg_dict.keys():
            for sensor in mov_avg_dict.keys():
                if "Lube" in sensor and "Post" not in sensor and "Pre" not in sensor:
                    mov_avg_dict[sensor]["Alert"]="No change in post filter pressure"
    elif press_number=="11":
        if "Lub_PostFltr_Pre" not in mov_avg_dict.keys():
            for sensor in mov_avg_dict.keys():
                if "Lub" in sensor and "Post" not in sensor and "Pre" not in sensor:
                    mov_avg_dict[sensor]["Alert"]="No change in post filter pressure"
    return mov_avg_dict



def check_temp_diff(press_number):
    # From DB get the collection list. For press 24 everything is in one batch
    pressdb = myclient_global[f'Press_{press_number}']
    if press_number=="24":
        press_angle_tag_name = 'Press_Angle'
        collection_list = ["BATCH_1"]
    elif press_number=="11":
        press_angle_tag_name = 'Press_angle'
        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
    
    db_keys_list = []
    batch_debug_dict={}
    QUERY = {"Date": {'$gte': time_an_hour_before, '$lt':  time_now}}
    for collection in collection_list:
        val=[]
        allkeysbatch1 = pressdb[collection].find().sort('_id',-1)
        db_keys_list = list(allkeysbatch1[0].keys())
        db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date"]#* List of all datapoints in the database
        db_keys_list = set(db_keys_list)
        val = [i for i in db_keys_list if "Temp" in i or "tmp" in i]
        batch_debug_dict[collection]=val    #gets batch dictionary only for temperature sensors
    dict_temp={}
    for batch,value in batch_debug_dict.items():
        for col in value:
            try:
                collection          = pressdb[batch]
                QUERY               = { "Date"      : {'$gte': time_an_hour_before, '$lt':  time_now}}
                document_count = collection.count_documents(QUERY)
                projection= {"_id":0,"Date":1,col:1}
                results= collection.find(QUERY,projection)
                df= pd.DataFrame(results).set_index("Date")
                dict_temp[col]=(df[col].iloc[document_count-1]-df[col].iloc[0]).round(4)
            except:
                print("Data not collected for", col)
                print()
    lst_alert={key:value for key,value in dict_temp.items() if abs(value)>=5}
    # TODO: read the alert from database and check in last hour we got the alert for same datapoint
    # if yes, stop the press

    return lst_alert

def count_lube_peak(press_number):
    press=f"Press_{press_number}"
    press_db = myclient_global[press]
    db_keys_list = []
    #only for testing until all batch updates are completed for press_24
    # if press_number=="24":
    #     collection_list=["BATCH_3","BATCH_4"]
    # else:
    collection_list=press_db.list_collection_names()
    collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
    for collection in collection_list:
        # if press_number=="24":
        #     QUERY1 = {"Date": {'$gte': datetime(2023,5,23,10,0,0), '$lt':  datetime(2023,5,23,10,1,0)}}
        #     allkeysbatch1 = press_db[collection].find(QUERY1).limit(1)
        # else:
        allkeysbatch1 = press_db[collection].find().sort('_id',-1)
        db_keys_list += list(allkeysbatch1[0].keys())
    db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date"]#* List of all datapoints in the database
    db_keys_list = set(db_keys_list)

    sensor_list=[]
    #merge into one condition "lub" when updates are done
    if press_number=="11":
        sensor_list=["Lubrication_B_05_Pressure_psi","Cr_LBlock1_Pre","Lub_PreFltr_Pre","Lub_PostFltr_Pre"]
    else:
        for sensor in db_keys_list:
            #if "lub" in sensor:
            if "Lube" in sensor or "lub" in sensor:
                if ("Vrms" not in sensor) and ("Arms" not in sensor) and ("Temp" not in sensor) and ("Apeak" not in sensor) and ("apk" not in sensor)  and ("crst" not in sensor) and ("vrms" not in sensor) and ("arms" not in sensor) and ("tmp" not in sensor):
                    sensor_list.append(sensor)
    if press_number=="11" or press_number== "21":
        batches=[1,2]
    elif press_number=="05":
        return None
    else:
        batches=[1,2,3,4]
    
    batch_debug_dict={}
    for column in sensor_list:
        for batch in batches:
            collection = "BATCH_"+str(batch)
            db = press_db[collection]
            # if press_number=="24":
            #     QUERY={"Date":{"$gte":datetime(2023,5,23,10,0,0), "$lt": datetime(2023,5,23,10,1,0)}}
            #     results = db.find(QUERY).limit(1)
            # else:
            results=db.find().sort('_id',-1)
            recent_doc = results[0]
            if column in recent_doc.keys():
                if batch not in batch_debug_dict:
                    batch_debug_dict[batch] = [column]
                else:
                    batch_debug_dict[batch].append(column)
    
    press_angle_list=[[340,90],[90,180],[180,270],[270,340]]
    if press=="Press_24":
        pressangle="Press_Angle"
    elif press=="Press_11":
        pressangle="Press_angle"
    peak_d={}
    for key,value in batch_debug_dict.items():
        lst_temp=value
        for col in lst_temp:
            peak_d[col]={"Average peaks":{}, "Average pressure":{}}
    start = time_an_hour_before
    end   = time_now
    for key,value in batch_debug_dict.items():
        batch="BATCH_"+str(key)
        lst_temp=value
        for col in lst_temp:
            collection          = press_db[batch]
            projection          = {"_id":0,"Date":1,col:1}
            if press_number=="24" or press_number=="11":
                for angle in press_angle_list:
                    #try:
                        avg_peaks=[]
                        for beg_time in DateTimeRange(start,end).range(timedelta(minutes=1)):   #number of peaks for each minute in the past hour
                            if angle[0]==340 and angle[1]==90:
                                end_time=beg_time+timedelta(minutes=1)
                                if end_time in DateTimeRange(start,end):
                                    QUERY1               = { '$and' : [{"Date": {'$gte': beg_time, '$lt':  end_time}}, {pressangle : {'$gte':340, '$lte': 360}}]}
                                    QUERY2 = { '$and' : [{"Date": {'$gte': beg_time, '$lt':  end_time}}, {pressangle : {'$gte':0, '$lt': 90}}]}  #340-90 coming down angles
                                    results1= collection.find(QUERY1,projection)
                                    results2=collection.find(QUERY2,projection)
                                    df1= pd.DataFrame(results1)
                                    df2=pd.DataFrame(results2)
                                    df=pd.concat([df1,df2])
                                    if df.empty==False:
                                        inp=df[col].to_numpy()
                                        peaks,dict_peaks=find_peaks(inp,height=5)  #static threshold, cann be changed as per new data
                                        avg_peaks.append(len(peaks))
                                    else:      
                                        continue                
                            else:
                                end_time=beg_time+timedelta(minutes=1)
                                if end_time in DateTimeRange(start,end):
                                    QUERY              = { '$and' : [{"Date": {'$gte': beg_time, '$lt':  end_time}}, {pressangle : {'$gte':angle[0], '$lt': angle[1]}}]}
                                    results= collection.find(QUERY,projection)
                                    df= pd.DataFrame(results)
                                    if df.empty==False:
                                        inp=df[col].to_numpy()
                                        peaks,dict_peaks=find_peaks(inp,height=5)  #static threshold, cann be changed as per new data
                                        avg_peaks.append(len(peaks))
                                    else:
                                        continue  
                        if len(avg_peaks)!=0 and len(dict_peaks["peak_heights"]!=0):
                            peak_d[col]["Average peaks"][str(angle[0])+"-"+str(angle[1])]=(round(sum(avg_peaks)/len(avg_peaks),2))   #avg number of peaks in the last hour per minute
                            peak_d[col]["Average pressure"][str(angle[0])+"-"+str(angle[1])]=(round(sum(dict_peaks["peak_heights"])/len(dict_peaks["peak_heights"]),2))

            else:
                try:
                    avg_peaks=[]
                    for beg_time in DateTimeRange(start,end).range(timedelta(minutes=1)):  #number of peaks for each minute in the past hour
                        collection          = press_db[batch]
                        projection          = {"_id":0,"Date":1,col:1}
                        end_time=beg_time+timedelta(minutes=1)
                        if end_time in DateTimeRange(start,end):
                            QUERY1               = {"Date": {'$gte': beg_time, '$lt':  end_time}}
                            results1= collection.find(QUERY1,projection)
                            df= pd.DataFrame(results1).set_index("Date")
                            inp=df[col].to_numpy()
                            peaks,dict_peaks=find_peaks(inp,height=5)  #static threshold, cann be changed as per new data
                            avg_peaks.append(len(peaks))
                    peak_d[col]["Average peaks"]=(round(sum(avg_peaks)/len(avg_peaks),2))    #avg number of peaks in the last hour per minute
                    peak_d[col]["Average pressure"]=(round(sum(dict_peaks["peak_heights"])/len(dict_peaks["peak_heights"]),2))
                except: 
                    continue
    alert_peak_change={}
    if press_number=="24" or press_number=="11":
        for sensor in sensor_list:
            if "fl" in sensor or "fr" in sensor or "rr" in sensor or "rl" in sensor:
                element_lst=sensor.split("_")
                core_ele=element_lst[1][2:]
                for lub_key in peak_d.keys():
                    if lub_key!=sensor and core_ele in lub_key:
                        for i in range(len(press_angle_list)):
                            try:
                                if peak_d[sensor]["Average peaks"]!=0:
                                    if (abs(peak_d[lub_key]["Average peaks"][str(press_angle_list[i][0])+"-"+str(press_angle_list[i][1])]-peak_d[sensor]["Average peaks"][str(press_angle_list[i][0])+"-"+str(press_angle_list[i][1])])
                                        /peak_d[sensor]["Average peaks"][str(press_angle_list[i][0])+"-"+str(press_angle_list[i][1])])*100>30:

                                        alert_peak_change[sensor]=peak_d[lub_key]
                            except Exception as E:
                                print(E)

    return peak_d,alert_peak_change

##############################################
#### Check if same alert already exists ######
##############################################

def check_if_alert_duplicate(press):
    try: #edge case of no previous documents returns that there is no duplicate
        previous_document_from_db = alert_db[f"Press_{press}"].find().limit(1).sort("$natural", -1)
        previous_document_from_db = previous_document_from_db[0]
    except:
        return 0
    previous_document_time = previous_document_from_db['_id']
    if previous_document_time == start_of_the_alert:
        return 1
    else:
        return 0
    

def get_result(press_number:str):
    logging.info("--> get_results(): Checking if press is running or not")
    press_status = is_press_running(press_number)
    logging.info(f"--> get_results() for {press_number}: Press Status {press_status}")
    if press_status:
        job = get_job(press_number)
        logging.info(f"--> get_results(): Currently running Job {job}")
        
        const_data=check_constantdata(press_number)
        logging.info("--> get_results(): COMPLETED check_constantdata()....")

        temp_diff=check_temp_diff(press_number)
        logging.info("--> get_results(): COMPLETED Executing check_temp_diff()....")

        miss_list=check_missing_datapoints(press_number)
        logging.info("COMPLETED check_missing_datapoints....")
                                        
        # # Get static threshold for the press
        # get_previous_thresholds(press)

        # print("Executing check_component_moving_averag()....")
        
        mov_avg_dict=check_component_moving_average(press_number)
        logging.info("--> get_results(): COMPLETED check_component_moving_average()....")

        # logging.debug("COMPLETED check_component_moving_averag()....")

        
        
        

        
        

        if press_number=="24":
            lube_peak=count_lube_peak(press_number)[0]
            peak_change=count_lube_peak(press_number)[1]
        #     result_dict={"Press Number": press, "Broker Connection":Broker_connections[press], "Database Connection":Db_connections[press], "Job Id":job ,"Missing data":miss_list, "Constant data check":const_data,"Moving average difference":mov_avg_dict, "Temperature change":temp_diff, 
        #             "Number of peaks":lube_peak, "Change in peaks": peak_change}
        # elif press=="05":
        #     result_dict={"Press Number": press, "Broker Connection":Broker_connections[press], "Database Connection":Db_connections[press], "Missing data":miss_list, "Constant data check":const_data,"Moving average difference":mov_avg_dict, "Temperature change":temp_diff}
        else:
            lube_peak=count_lube_peak(press_number)[0]
            peak_change=[]
        result_dict={"Press Number": press_number,
                     "Press Status":press_status,
                     "Job Id":job,
                     "Broker Connection":Broker_connections[press_number], 
                     "Database Connection":Db_connections[press_number], 
                     "Missing data":miss_list, 
                     "Constant data check":const_data,
                     "Moving average difference":mov_avg_dict, 
                     "Temperature change":temp_diff, 
                     "Number of peaks":lube_peak, 
                     "Change in peaks": peak_change
                    # "Number of peaks":lube_peak
                    }
        
    else:
        temp_diff=check_temp_diff(press_number)
        logging.info("COMPLETED Executing check_temp_diff()....")
        miss_list=check_missing_datapoints(press_number)
        logging.info("COMPLETED check_missing_datapoints....")
        result_dict={"Press Number": press_number,
                     "Press Status":press_status,
                     "Broker Connection":Broker_connections[press_number], 
                     "Database Connection":Db_connections[press_number], 
                     "Missing data":miss_list,  
                     "Temperature change":temp_diff, 
                    #  "Number of peaks":lube_peak
                     }
    return result_dict


##############################################
######### Assign Warnings to PLC #############
##############################################
def PLC_warning_signal():
    if alert_code != 0:
        error_val = 0
        for i in alert_code["Temperature change"]: # dictionary with the sensor as the key and the temperature it changes by, make it error_val 2 is greater than 10 otherwise do 1
            if abs(alert_code["Temperature change"][i])  >= 10:
                error_val = critical_alert_code
            else:
                error_val = max(error_val, warning_code)
        for i in alert_code["Number of peaks"]:
            if i: #put correct condition
                error_val = max(error_val, warning_code)
        for i in alert_code["Moving average difference"]: #dictionary with the sensor as key and a percentage as a change (30 is the threshold)
            if abs(alert_code["Moving average difference"][i]) > 50 : #put correct condition
                error_val = critical_alert_code
            else:
                error_val = max(error_val,warning_code)
        
        return error_val


##############################################
####### Send Valid alerts to DB and PLC ######
##############################################

def send_alerts_to_DB_and_PLC(press_number):
    alert_db = myclient_global["Press_Alerts"]
    alert_collection = alert_db[f"Press_{press_number}"]
    logging.info("--> Executing send_alerts_to_DB_and_PLC()")
    try:
        # Save the alerts in database
        if len(alert_code)!=0: #If there is any alert in dictionary 
            alert_code.update({"_id":start_of_the_alert}) # Date as main index to avoid duplicate key error
            try:     #edge case of no entries in the database
                alert_collection.insert_one(alert_code)
                # previous_alert = alert_collection.find().limit(1).sort("$natural", -1)
            except: 
                logging.info("--> send_alerts_to_DB_and_PLC(): Not connected to alert_collection")    
            #query documents with the same date delete old document and add it to a new dictionary if it don't exist and then add the new dictionary                    
            alert_code.update({"_id":start_of_the_alert})
            alert_collection.insert_one(alert_code)

            # add to plc needs to be editted for new thresholds
            # if press_chosen == '05':
            #     mqttc_21_and_05.publish("Press_05_alert", PLC_warning_signal())
            # elif press_chosen == '11':
            #     mqttc_11.publish("Press_11_alert", PLC_warning_signal())
            # elif press_chosen == '21':
            #     mqttc_21_and_05.publish("Press_21_alert", PLC_warning_signal())
            # elif press_chosen == '24':
            #     mqttc_24.publish("Press_24_alert", PLC_warning_signal() )

            print("Alerts for the entire hour added")

        else:
            if press_chosen == '05':
                mqttc_21_and_05.publish("Press_05_alert","No Alerts/Warnings")    
            elif press_chosen == '11':
                mqttc_11.publish("Press_11_alert","No Alerts/Warnings")    
            elif press_chosen == '21':
                mqttc_21_and_05.publish("Press_21_alert","No Alerts/Warnings")    
            elif press_chosen == '24':    
                mqttc_24.publish("Press_24_alert","No Alerts/Warnings")
            
            
    except Exception as e:
        print(e) 

# def send_alerts_to_PLC():
    

##############################################
######### Execute Script on Run #############
##############################################
i=0
while True:
    time_now = datetime.now()
    time_an_hour_before = datetime.now()-timedelta(minutes=60)
    time_half_an_hour_before = time_now - timedelta(minutes=30)
    time_next = time_now + timedelta(minutes=1)
    current_hour = time(datetime.now().hour)
    start_of_the_alert = datetime.combine(datetime.now(), current_hour)

    logging.debug(f"Time to Evaluate all Presses for {time_now}")
    logging.debug("Executing check_PLC_MQTT_DB_connection")
    # Check Broker and DB status
    for press_number in ["11","24"]:         #presses_and_IP:
        Overall_connections = check_PLC_MQTT_DB_connection(
                                        press_number    = press_number, 
                                        host            = presses_and_IP[press_number] 
                                        )
        start_time=datetime.now()
        connection_status = list(Overall_connections.values())[0]
        # Generate alerts only if connections are good
        # if connection_status:
        print(f"--> Generating alerts for Press{press_number}")
        # Check if alert for that hour exists in database
        if check_if_alert_duplicate(press_number):
            logging.info("-->Alert with the same time already exists in Alert database!")
            continue
        else:
            alert_code = get_result(press_number)
            print(alert_code)
        send_alerts_to_DB_and_PLC(press_number)
        print("-"*100)
        
    i+=1
    
    # delete after
    '''
    for press_chosen in Overall_connections:
        start_time=datetime.now()
        print(f"Generating alerts for Press_{press_chosen}")

        if press_chosen == '11':
            mqttc_11.loop_start()
        elif press_chosen == '24':
            mqttc_24.loop_start()
        elif press_chosen == '05' or press_chosen == '21':
            mqttc_21_and_05.loop_start()
        
        if check_if_alert_duplicate(press_chosen):
            logging.warning("Document with the same time already exists!")
            continue
        try:
            alert_code = get_result(press_chosen)
            alert_collection = alert_db[f"Press_{press_chosen}"]
            send_alerts_to_DB_and_PLC()
        except Exception as e:
            print(f"Unable to generate results for Press_{press_chosen} because of error: {e}")
    
        end_time=datetime.now()
        print(press_chosen,": ", start_time, end_time)  #only for check



if press_chosen == '24':
    mqttc_24.disconnect()
elif press_chosen == '11':
    mqttc_11.disconnect()
elif press_chosen == '05' or press_chosen == '21':
    mqttc_21_and_05.disconnect()
    
        '''
