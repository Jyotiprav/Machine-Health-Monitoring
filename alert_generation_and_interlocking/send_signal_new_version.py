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
import subprocess
import logging
logging.basicConfig(level=logging.DEBUG)



######################################################################
######### Class to initialize all instances of the PLC broker ########
######################################################################
class init_broker:
    def __init__(self) -> None:
        self.port = 1883
        self.keep_alive = 60

        self.Press_24_topic = 'Press_24/#'
        self.Press_21_topic = 'Press_21/#'
        self.Press_11_topic = 'Press_11/#'
        self.Press_05_topic = 'Press_05/#'

        self.mqttc_24 = mqtt.Client(client_id="Jetson alert_check_client_p24_test1", clean_session=False)
        self.mqttc_21_and_05 = mqtt.Client(client_id="Jetson alert_check_client_p21_and_p05_test1", clean_session=False)
        self.mqttc_11 = mqtt.Client(client_id="Jetson alert_check_client_p11_test1", clean_session=False)

        self.broker_connections = generate_send_signal(threshold_sheet,live_sensor_sheet,ip_sheet, sensor_threshold_percent, mongo_IP).broker_connections
        self.presses_and_IP = generate_send_signal(threshold_sheet,live_sensor_sheet,ip_sheet, sensor_threshold_percent, mongo_IP).presses_and_IP

    #########################################################################################################################################################################
    ##### Helper functions that all are called when certain actions of the broker happen (i.e on_connect for when it connects, on_publish when something publishes, etc. ####
    #########################################################################################################################################################################
    
    def on_connect_24(self,client, userdata, flags, rc):
        if ( rc == 0 ):
            print("Connected to Mqtt Broker successfully")
            self.mqttc_24.subscribe(self.Press_24_topic,0)
            # client.subscribe([(Press_24_topic, 0), ("another/topic", 0)])
        else:                   
            print("Connect returned result code: " + str(rc))

    def on_connect_21_and_05(self,client, userdata, flags, rc):
        if ( rc == 0 ):
            print("Connected to Mqtt Broker successfully")
            # client.subscribe(Press_24_topic,0)
            self.mqttc_21_and_05.subscribe([(self.Press_21_topic, 0), (self.Press_05_topic, 0)])
        else:                   
            print("Connect returned result code: " + str(rc))

    def on_connect_11(self,client, userdata, flags, rc):
        if ( rc == 0 ):
            print("Connected to Mqtt Broker successfully")
            self.mqttc_11.subscribe(self.Press_11_topic,0)
            # client.subscribe([(Press_24_topic, 0), ("another/topic", 0)])
        else:                   
            print("Connect returned result code: " + str(rc))

    def on_publish(self,client,userdata,results):    
        # what do you want to happen when something is published
        pass

    def on_message(self,client, userdata, msg):
        # what do you want to happen when a message is sent (basically always)
        pass
########################################################################################        
################################# Main function of this class ##########################
########################################################################################
    
    def configure_broker(self):
        for working_press in self.broker_connections:

            if working_press == '24' and self.broker_connections[working_press]:
                    try: 
                        self.mqttc_24.on_publish = self.on_publish
                        self.mqttc_24.on_connect = self.on_connect_24
                        self.mqttc_24.on_message = self.on_message
                        self.mqttc_24.connect(self.presses_and_IP['24'], int(self.port), int(self.keep_alive))
                    except:
                        logging.error(f"Did not connect to Press_{working_press}")
                        self.broker_connections[f"Press_{working_press}"] = False

            # CONFIGURE mqtt client for Press 21 and 5
            if (working_press == '21' or working_press =='05') and self.broker_connections[working_press]:
                try:
                    self.mqttc_21_and_05.on_publish = self.on_publish
                    self.mqttc_21_and_05.on_connect = self.on_connect_21_and_05
                    self.mqttc_21_and_05.on_message = self.on_message
                    self.mqttc_21_and_05.connect(self.presses_and_IP['21'], int(self.port), int(self.keep_alive))
                except:
                    logging.error(f"Did not connect to Press_{working_press}")
                    self.broker_connections[f"Press_{working_press}"] = False

            # CONFIGURE mqtt client for Press 11
            if working_press == '11' and self.broker_connections[working_press]:
                try:
                    self.mqttc_11.on_publish = self.on_publish
                    self.mqttc_11.on_connect = self.on_connect_11
                    self.mqttc_11.on_message = self.on_message
                    self.mqttc_11.connect(self.presses_and_IP['11'], int(self.port), int(self.keep_alive))
                except:
                    logging.error(f"Did not connect to Press_{working_press}")
                    self.broker_connections[f"Press_{working_press}"] = False


###############################################################################################################################
################## Class for generating alerts and sending it to Broker/DB ####################################################
###############################################################################################################################

class generate_send_signal:
    
    def __init__(self, threshold_sheet: str, live_sensor_sheet:str, ip_sheet:str, sensor_threshold_percent:dict, mongo_IP:str):
        #iniatialize all sheets
        self.threshold_sheet = threshold_sheet
        self.live_sensor_sheet = live_sensor_sheet
        self.excel_file_with_ips = ip_sheet

        #initialize threshold
        self.sensor_threshold_percent = sensor_threshold_percent
        
        #initialize the dictionary that takes has the press as a key and points to the IP of the press
        ip_data = pd.ExcelFile(self.excel_file_with_ips)
        self.presses_and_IP = {} # Pass the list of IPs as parmeters from main. Make it read the IPS from excel or csv file. #done
        #code that parses through the IP_list excel file to get all the presses and their corresponding IPs
        df = ip_data.parse(index_col='Press #')  
        temp_dict = df.to_dict()['Broker IP']
        for num in temp_dict:
            if int(num) < 10:
                self.presses_and_IP[f"0{num}"] = temp_dict[num]
                continue
            self.presses_and_IP[f"{num}"] = temp_dict[num]
        # self.presses_and_IP{f"{num}":temp_dict[num] for num in temp_dict}

        #initialize the mongodb database
        self.myclient_global = pymongo.MongoClient(mongo_IP) 
        self.alert_db = self.myclient_global["Press_Alerts"]

        #initialize connection dictionaries
        self.broker_connections = {}
        self.db_connections = {}
        self.overall_connections = {}
        for press in self.presses_and_IP:
            self.broker_connections[press], self.db_connections[press], self.overall_connections[press] = False, False, False
        
        #initiate warning/critical         
        self.critical_code                   = 2
        self.warning_code                    = 1

        #initialize times
        self.time_now = datetime.now()
        self.time_an_hour_before = datetime.now()-timedelta(minutes=60)
        self.time_half_an_hour_before = self.time_now - timedelta(minutes=30)
        self.time_next = self.time_now + timedelta(minutes=1)
        current_hour = time(datetime.now().hour)
        self.start_of_the_alert = datetime.combine(datetime.now(), current_hour)
         

    def __repr__(self) -> str: # a means to check all your instances
         return f""
        
    #############################################################################
    ###### Accessing the gloval variable of getting previous thresholds #########
    #############################################################################
    
    def get_previous_thresholds(self):
        global previous_threshold_global
        self.previous_threshold = previous_threshold_global
        return self.previous_threshold
    

##############################################
######### Check all valid Connections  ########
##############################################
    
    def check_PLC_MQTT_DB_connection(self) -> None:
        try: 
            from time import sleep
            
            for press_number in self.presses_and_IP:
                sleep(3)
                print("testing print line number 116",self.presses_and_IP)
                host = self.presses_and_IP[press_number] #put the IP of the plc here        
                param = '-n' if platform.system().lower()=='windows' else '-c'
            # Building the command. Ex: "ping -c 1 google.com"
                command = ['ping', param, '1' '''#How many packets you want to send during the ping''' , host]
                if subprocess.call(command) != 0:
                    logging.error("PLC to Broker Connection Error.")
                    self.broker_connections[press_number] = False
                    logging.warning(f"Check the PLC connection for Press_{press_number} with the IP of {self.presses_and_IP[press_number]}")
                    print()
                else:
                    self.broker_connections[press_number] = True

                pressdb = self.myclient_global[f'Press_{press_number}']        
                collection_list = pressdb.list_collection_names()
                collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings        
                first_batch_db = pressdb[collection_list[0]]        
                current_time = datetime.now().replace(second=0, microsecond=0)       
                query = {'Date' : {'$gte' : current_time}}    
                logging.info(f"Getting {current_time} document from the database...")     
                result = None        
                result = first_batch_db.find_one(query)
                if not result:        
                    logging.error("MQTT to DB Connection broken.")
                    logging.warning(f"Check the DB connection for Press_{press_number}.")
                    self.db_connections[press_number] = False
                    print()
                
                    for last_update in first_batch_db.find().sort('$natural',-1).limit(1):        
                        logging.warning(f"The last doc added for Press_{press_number} was at {last_update['Date']}")
                        print()

                else:
                    logging.info(f"All Connections for Press_{press_number} Good!")
                    self.overall_connections[press_number], self.broker_connections[press_number], self.db_connections[press_number] = True, True, True
                
                print("__________________________________________________________________________________________________________")
            print("Done Checking all Connections")
        
        except:
            logging.critical("Network/Database Down!")


#########################################################
###### Get Job from DB ##################################
#########################################################
    
    def get_job(self,press):
        if press == '24':
            pressdb = self.myclient_global[f'Press_{press}']
            recent_document_from_db = pressdb["BATCH_1"].find().limit(1).sort("$natural", -1)
            recent_document_from_db = recent_document_from_db[0]
            recent_job = recent_document_from_db['job_id']
            if recent_job == '':
                return "No Job Inputted"
            return recent_job
        else: return None


    def check_missing_datapoints(self, press_number:str):
        pressdb = self.myclient_global[f'Press_{press_number}']
        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]   #* List of all collection names as strings, filters other collections
        db_keys_list = []

        QUERY = {"Date": {'$gte': self.time_an_hour_before, '$lt': self.time_now}}
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

    def is_press_really_running(self, press_number:str):
        print("Executing is_press_really_running()...")
        pressdb = self.myclient_global[f'Press_{press_number}']
        # get all the collection name in database
        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
        end = self.time_now
        start = self.time_an_hour_before
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
            if press_number=="24":
                df['angle_diff'] = df['Press_Angle'].diff()
                significant_changes = df['angle_diff'].abs() > 0.001
                filtered_df = df[significant_changes]
                filtered_df = filtered_df.drop(["angle_diff"], axis = 1)
                if filtered_df.empty:
                    return False
                return True
            elif press_number=="11":
                df['angle_diff'] = df['Press_angle'].diff()
                significant_changes = df['angle_diff'].abs() > 0.001
                filtered_df = df[significant_changes]
                filtered_df = filtered_df.drop(["angle_diff"], axis = 1)
                if filtered_df.empty:
                    return False
                return True
            else:
                if df.empty:
                    return False
                return True        

    def is_press_running(self,press_number:str):
        press_angle_tag_name =""
        if press_number=="24":
            press_angle_tag_name = 'Press_Angle'
        elif press_number=="11":
            press_angle_tag_name = 'Press_angle'

        pressdb = self.myclient_global[f'Press_{press_number}']
        # get all the collection name in database
        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
        end = self.time_now
        start = self.time_an_hour_before
        errorlist = []
        QUERY = {"Date": {'$gte': start, '$lt':  end}}
        for collection in collection_list[0]:
            allkeysbatch1 = pressdb[collection].find().sort('_id',-1)
            db_keys_list = list(allkeysbatch1[0].keys())
            db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date" and i != "Ram_Adjust_Chain_On/Off" and "press" not in i and i!="job_id"]#* List of all datapoints in the database
            db_keys_list = set(db_keys_list)
            projection = {'_id': 0, 'Date': 1, **{x: 1 for x in db_keys_list}}
            results = pressdb[collection].find(QUERY, projection)
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
    def check_constantdata(self,press_number:str):
        logging.info("\t\tExecuting check_constantdata()....")
        pressdb = self.myclient_global[f'Press_{press_number}']

        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
        end = self.time_now
        start = self.time_an_hour_before
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

            if press_number=="24":
                #Filters downtime since press angle remains constant when press not running
                df['angle_diff'] = df['Press_Angle'].diff()
                significant_changes = df['angle_diff'].abs() > 0.001
                filtered_df = df[significant_changes]
                filtered_df = filtered_df.drop(["angle_diff"], axis = 1)
    
                if filtered_df.empty:
                    print(f'Press{press_number} not running')
                    errorlist+="Press is not running."
                else:
                    filtered_df_diff = filtered_df.diff().abs()
                    sum_diff = filtered_df_diff.sum(axis=0)
                    for x in db_keys_list:
                        if sum_diff[x] == 0 and "fdl" not in x:
                            errorlist += [x]
            elif press_number=="11":
                df['angle_diff'] = df['Press_angle'].diff()
                significant_changes = df['angle_diff'].abs() > 0.001
                filtered_df = df[significant_changes]
                filtered_df = filtered_df.drop(["angle_diff"], axis = 1)
                if filtered_df.empty:
                    print(f'Press{press_number} not running')
                    errorlist+="Press is not running."
                else:
                    filtered_df_diff = filtered_df.diff().abs()
                    sum_diff = filtered_df_diff.sum(axis=0)
                    for x in db_keys_list:
                        if sum_diff[x] == 0  and "fdl" not in x:
                            errorlist += [x]
            else:
                if df.empty:
                    print(f'Press{press_number} not running')
                else:
                    df_diff = df.diff().abs()
                    sum_diff = df_diff.sum(axis=0)
                    for x in db_keys_list:
                        if sum_diff[x] == 0  and "fdl" not in x:
                            errorlist += [x]
        return errorlist

    # component based threshold check function on 87% of data removing outliers with quantiles. 
    def check_component_threshold(self,component = "Ram_Adj_Mtr"):
        pass
        
        
        
    # check for thresholds and set them. Thresholds are based on the data we got in march
    def get_threholds(self,press_number,component,startdate,enddate):
        # thresholds are based on job run 597_598 in march . 
        # datapoints to check
        if "Pre" in component or "psi" in component:
        #if "psi" in component:
            list_of_datapoints=[component]
        else:
            list_of_datapoints = [component+"_Vrms",component+"_Arms",component+"_Apk",component+"_Crst",component+"_Crest",component+"_Apeak",component+"_Pre",
                                component+"_vrms",component+"_arms",component+"_apk",component+"_crst",component+"_psi"]
        pressdb = self.myclient_global[f'Press_{press_number}']
        # Get component's batch/collection
        collection_list = pressdb.list_collection_names() #List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
        list_of_sensor_in_collection = []
        threshold_dict ={}
        for collection in collection_list:
            allkeysbatch1 = pressdb[collection].find().sort('_id',-1)
            db_keys_list=list(allkeysbatch1[0].keys())
            list_of_sensor_in_collection = [sensor for sensor in list_of_datapoints if sensor in db_keys_list]
            if list_of_sensor_in_collection !=[]:
                for sensor in list_of_sensor_in_collection:

                    if press_number == "24":
                        
                        if (collection=="BATCH_4") and startdate> datetime(2023,3,31) and startdate<=datetime(2023,10,4):
                            current_dir=os.getcwd()
                            sensorsheet=pd.read_excel((live_sensor_sheet), sheet_name= "Press_24", usecols="E,H")
                            old_lst=sensorsheet["Old labels"].tolist()
                            new_lst=sensorsheet["Updated labels"].tolist()
                            if sensor in new_lst:
                                sensor_name=old_lst[new_lst.index(sensor)]
                            else:
                                sensor_name=sensor
                            
                            if "Vrms" in sensor or "vrms" in sensor:
                            #if "vrms" in sensor:
                                    QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor_name : {'$lte': 3}}]}
                            elif "Arms" in sensor or "Apk" in sensor or "Apeak" in sensor or "Crst" in sensor or "arms" in sensor or "apk" in sensor or "crst" in sensor:
                            #elif "arms" in sensor or "apk" in sensor  or "crst" in sensor
                                    QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor_name : {'$lte': 3000}}]}
                            else:
                                    QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor_name : {'$lte' : 10000}}]}
                            projection = {'_id':0, 'Date':1, "Press_Angle":1,sensor_name:1}
                            results = pressdb[collection].find(QUERY, projection)      
                            df = pd.DataFrame(results).set_index("Date")

                            #Filter the downtime
                            df['angle_diff'] = df["Press_Angle"].diff()
                            significant_changes = df['angle_diff'].abs() > 0.001
                            filtered_df = df[significant_changes]
                            filtered_df = filtered_df.drop(["angle_diff", "Press_Angle"], axis = 1)
                            threshold_dict.update({sensor : (filtered_df[sensor_name].rolling(window = timedelta(seconds=15),center = True).mean()).mean()})

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
        return threshold_dict

    # component moving average check
    def check_component_moving_average(self,press_number:str):        
        startdate = self.time_an_hour_before
        enddate = self.time_now
        pressdb = self.myclient_global[f'Press_{press_number}']

        # get all the collection name in database
        db_keys_list = []
        comp_lst=[]
        collection_list = pressdb.list_collection_names()  #* List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
        QUERY = {"Date": {'$gte': datetime(2023,5,23,10,0,0), '$lt':  datetime(2023,5,23,10,1,0)}}
        const_data_lst=self.check_constantdata(press_number)
        for collection in collection_list:
            allkeysbatch1=pressdb[collection].find().sort('_id',-1)
            db_keys_list += list(allkeysbatch1[0].keys())
            db_keys_list  = [i for i in db_keys_list if i!="_id" and i!="Date" and i!="Ram_Adjust_Chain_On/Off" and i!="job_id" and i not in const_data_lst]#* List of all datapoints in the database

        db_keys_list = set(db_keys_list)
        for i in db_keys_list:
            if "Vrms" in i or "Arms" in i or"Crest" in i or "Apk" in i or "Crst" in i or "Apeak" in i or "vrms" or "arms" or  "crst" or "apk" in i:
                code_list=i.split("_")
                comp="_".join(code_list[:len(code_list)-1])
                if comp not in comp_lst:
                    comp_lst.append(comp)
            else:
                comp_lst.append(i)
        mov_avg_dict={}
        for component in comp_lst:
            try:
                if press_number == "21":
                    #threshold_dict = get_threholds(press_number,component,datetime(2023,5,2,12,45,0),datetime(2023,5,3,4,0,0))
                    massive_dict = self.get_previous_thresholds().get(press_number, None) or {}
                    threshold_dict = massive_dict.get(component, None) or self.get_threholds(press_number,component,datetime(2023,8,16,22,0,0),datetime(2023,8,17,10,45,0))  #732/33
                    massive_dict[component] = threshold_dict
                    recent_values = self.get_threholds(press_number,component,startdate,enddate)
                    for i in threshold_dict:
                        if (threshold_dict[i])!=0:
                            if abs(((recent_values[i]-threshold_dict[i])/(threshold_dict[i])))*100> 50:
                                mov_avg_dict[i]={"Rate of change":round(((recent_values[i]-threshold_dict[i])/(threshold_dict[i]))*100),"Threshold mean value":round(threshold_dict[i],3), "Recent mean values":round(recent_values[i],3)}

                elif press_number == "24":
                    job_start = None
                    job_end = None
                    job_start_date = None
                    job_end_date = None
                    thresholds = pd.read_excel(threshold_sheet,f"Press {press_number}").set_index("Jobs").to_dict()
                    list_of_all_jobs    = [i["Job Number"] for i in (pressdb["Job_details"].find())] 
                    first_batch_db = pressdb['BATCH_1']
                    current_job = None
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
                    previous_document_from_db = self.alert_db[f"Press_24"].find().limit(1).sort("$natural", -1)[0]
                    if previous_document_from_db['Job Id'] == self.get_job(press_number):
                        massive_dict = self.get_previous_thresholds().get(press_number, None) or {}
                    else: massive_dict = {}
                    if component =="hyd_unitMain":
                        threshold_dict = massive_dict.get(component, None) or self.get_threholds(press_number,component,job_start_date,job_end_date)
                        massive_dict[component] = threshold_dict
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
                        threshold_dict = massive_dict.get(component, None) or self.get_threholds(press_number,component,job_start_date,job_end_date)
                        massive_dict[component] = threshold_dict
                        recent_values = self.get_threholds(press_number,component,startdate,enddate)
                        for i in threshold_dict:
                            if (threshold_dict[i])!=0:
                                if "lub" in i or "Lube" in i or "Lub" in i:                #last 2 conditions only until batch updation complete for p21 and p11
                                    percent_change=sensor_threshold_percent["lub"]       # can customize percent threshold according to data type
                                else:
                                    percent_change=sensor_threshold_percent["vrms"]
                                if abs(((recent_values[i]-threshold_dict[i])/(threshold_dict[i])))*100> percent_change:
                                    mov_avg_dict[i]={"Rate of change":round(((recent_values[i]-threshold_dict[i])/(threshold_dict[i]))*100),"Threshold mean value":round(threshold_dict[i],3), "Recent mean values":round(recent_values[i],3)}
                elif press_number == "11":
                    # threshold_dict = get_threholds(press_number,component,datetime(2023,3,18,16,0,0),datetime(2023,3,18,22,0,0))  #press_11 threshold HYDGCPIA13
                    # threshold_dict = get_threholds(press_number,component,datetime(2023,8,25,17,0,0),datetime(2023,8,25,21,30,0))     #press_11 threshold 774_75
                    massive_dict = self.get_previous_thresholds().get(press_number, None) or {}
                    threshold_dict = massive_dict.get(component, None) or self.get_threholds(press_number,component,datetime(2023,8,30,15,0,0),datetime(2023,8,30,20,0,0))    #Press 11 threshold 748/49
                    massive_dict[component] = threshold_dict
                    recent_values = self.get_threholds(press_number,component,startdate,enddate)
                    for i in threshold_dict:
                        if (threshold_dict[i])!=0:
                                if abs(((recent_values[i]-threshold_dict[i])/(threshold_dict[i])))*100> 50:
                                    mov_avg_dict[i]={"Rate of change":round(((recent_values[i]-threshold_dict[i])/(threshold_dict[i]))*100),"Threshold mean value":round(threshold_dict[i],3), "Recent mean values":round(recent_values[i],3)}
                elif press_number == "05":
                    massive_dict = self.get_previous_thresholds().get(press_number, None) or {}
                    threshold_dict = massive_dict.get(component, None) or self.get_threholds(press_number,component,datetime(2023,9,28,0,0,0),datetime(2023,9,28,10,0,0))   #temporary reference job for press05
                    massive_dict[component] = threshold_dict
                    recent_values = self.get_threholds(press_number,component,startdate,enddate)
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
        return mov_avg_dict, massive_dict



    def check_temp_diff(self,press_number):
        logging.info("\t\tExecuting check_temp_diff...")
       
        
        
        # =========================================================================
        db_keys_list = []
        batch_info_dict={}
        pressdb = self.myclient_global[f'Press_{press_number}']
        # get all the collection name in database
        collection_list = pressdb.list_collection_names()  # List of all collection names as strings
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  # filtering the batch, avoiding any COPY databases
        QUERY = {"Date": {'$gte': self.time_an_hour_before, '$lt':  self.time_now}}
        # ===========================================================================
        # Get and Organize temperature sensors per batch
        #===========================================================================
        for collection in collection_list:
            val=[]
            latest_document         = pressdb[collection].find().sort('_id',-1)
            latest_document_keys    = list(latest_document[0].keys())
            latest_document_keys    = [i for i in latest_document_keys if i!="_id" and i!="Date"]# List of all datapoints in the database
            latest_document_keys    = set(latest_document_keys)     # remove duplicates
            val = [i for i in latest_document_keys if "Temp" in i or "tmp" in i] # Getting only temperature sensors
            batch_info_dict[collection]=val    #Organize temp sensors per batch
        #===========================================================================
        dict_temp={}
        dict_average_temp={}
        for batch,lst_temp in batch_info_dict.items():
            for col in lst_temp:
                try:
                    collection          = pressdb[batch]
                    QUERY               = { "Date"      : {'$gte': self.time_an_hour_before, '$lt':  self.time_now}}
                    document_count = collection.count_documents(QUERY)    
                    projection= {"_id":0,"Date":1,col:1}
                    results= collection.find(QUERY,projection)
                    df= pd.DataFrame(results).set_index("Date")
                    dict_temp[col]=(df[col].iloc[document_count-1]-df[col].iloc[0]).round(4)
                    dict_average_temp[col]=np.mean(df[col])
                except:
                    print("Data not collected for ", col)
                    print()
        lst_alert={}
        for key,value in dict_temp.items():
            if abs(value)>=5:                       #difference set at 5 degrees currently
                lst_alert[key]=value
        return lst_alert

    def count_lube_peak(self,press_number):
        press=f"Press_{press_number}"
        press_db = self.myclient_global[press]
        db_keys_list = []
        collection_list=press_db.list_collection_names()
        collection_list=[x for x in collection_list if "BATCH" in x and "COPY" not in x]  #* List of all collection names as strings
        for collection in collection_list:
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
                #if "lub" in sensor:  remove next condition once press 21 batches done updating 
                if "Lube" in sensor or "lub" in sensor: 
                    if ("Vrms" not in sensor) and ("Arms" not in sensor) and ("Temp" not in sensor) and ("Apeak" not in sensor) and ("apk" not in sensor)  and ("crst" not in sensor) and ("vrms" not in sensor) and ("arms" not in sensor) and ("tmp" not in sensor):
                        sensor_list.append(sensor)

        if press_number=="11" or press_number== "21":
            batches=[1,2]
        elif press_number=="05":
            return None
        else:
            batches=[1,2,3,4]
        batch_info_dict={}
        for column in sensor_list:
            for batch in batches:
                collection = "BATCH_"+str(batch)     #creating separate batch_info dict for lube sensors only format: {*batch_number:[list of lube sensors]}
                db = press_db[collection]
                results=db.find().sort('_id',-1)
                recent_doc = results[0]
                if column in recent_doc.keys():    #adds only lubrication sensors to the dictionary
                    if batch not in batch_info_dict:
                        batch_info_dict[batch] = [column]
                    else:
                        batch_info_dict[batch].append(column)
        
        press_angle_list=[[340,90],[90,180],[180,270],[270,340]]
        if press=="Press_24":
            pressangle="Press_Angle"
        elif press=="Press_11":
            pressangle="Press_angle"
        peak_d={}
        for key,value in batch_info_dict.items():
            lst_temp=value
            for col in lst_temp:
                peak_d[col]={"Average peaks":{}, "Average pressure":{}}  #creating peaks dictionary per sensor 
        start = self.time_an_hour_before
        end   = self.time_now
        for key,value in batch_info_dict.items():
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
                                        df=pd.concat([df1,df2])   #concatenates all values from 340-360 and 0-340 angle
                                        if df.empty==False:
                                            inp=df[col].to_numpy()
                                            peaks,dict_peaks=find_peaks(inp,height=5)  #static threshold, cann be changed as per new data
                                            avg_peaks.append(len(peaks))
                                        else:      
                                            continue                
                                else:
                                    end_time=beg_time+timedelta(minutes=1)
                                    if end_time in DateTimeRange(start,end):
                                        QUERY              = { '$and' : [{"Date": {'$gte': beg_time, '$lt':  end_time}}, {pressangle : {'$gte':angle[0], '$lt': angle[1]}}]}  #all values of the sensor in particular angles
                                        results= collection.find(QUERY,projection)
                                        df= pd.DataFrame(results)
                                        if df.empty==False:
                                            inp=df[col].to_numpy()
                                            #peaks=list of indexes of peaks, len(peaks)=number of peaks in the given timeframe
                                            #dict_peaks: dictionary with multiple properties of the signal, format: property: decription 
                                            peaks,dict_peaks=find_peaks(inp,height=5)  #static threshold, cann be changed as per new data
                                            avg_peaks.append(len(peaks))   
                                        else:
                                            continue  
                            if len(avg_peaks)!=0 and len(dict_peaks["peak_heights"]!=0):
                                peak_d[col]["Average peaks"][str(angle[0])+"-"+str(angle[1])]=(round(sum(avg_peaks)/len(avg_peaks),2))   #avg number of peaks in the last hour per minute
                                peak_d[col]["Average pressure"][str(angle[0])+"-"+str(angle[1])]=(round(sum(dict_peaks["peak_heights"])/len(dict_peaks["peak_heights"]),2))  #average pressure  in the last hour per minute

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
                                #peaks=list of indexes of peaks, len(peaks)=number of peaks in the given timeframe
                                #dict_peaks: dictionary with multiple properties of the signal, format: property: decription 
                                peaks,dict_peaks=find_peaks(inp,height=5)  #static threshold, cann be changed as per new data
                                avg_peaks.append(len(peaks))
                        peak_d[col]["Average peaks"]=(round(sum(avg_peaks)/len(avg_peaks),2))    #avg number of peaks in the last hour per minute
                        peak_d[col]["Average pressure"]=(round(sum(dict_peaks["peak_heights"])/len(dict_peaks["peak_heights"]),2))#average pressure in the last hour per minute
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
    
    def check_if_alert_duplicate(self,press):
        try: #edge case of no previous documents returns that there is no duplicate
            previous_document_from_db = self.alert_db[f"Press_{press}"].find().limit(1).sort("$natural", -1)
            previous_document_from_db = previous_document_from_db[0]
        except:
            return 0
        previous_document_time = previous_document_from_db['_id']
        if previous_document_time == self.start_of_the_alert:
            return 1
        else:
            return 0
        
    def get_result(self,press):
        
        const_data=self.check_constantdata(press)
        temp_diff=self.check_temp_diff(press)
        logging.info("\t\tExecuting check_component_moving_averag()....")
        mov_avg_dict, self.previous_threshold = self.check_component_moving_average(press)
        logging.info("Executing check_missing_datapoints....")
        miss_list=self.check_missing_datapoints(press)
        logging.info("Executing get_job()....")
        job = self.get_job(press)

        if press=="24":
            lube_peak=self.count_lube_peak(press)[0]
            peak_change=self.count_lube_peak(press)[1]
            result_dict={"Press Number": press, "Broker Connection":self.broker_connections[press], "Database Connection":self.db_connections[press], "Job Id":job ,"Missing data":miss_list, "Constant data check":const_data,"Moving average difference":mov_avg_dict, "Temperature change":temp_diff, 
                    "Number of peaks":lube_peak, "Change in peaks": peak_change}
            result_dict_with_connections = {"Press Number": press, "Broker Connection":self.broker_connections[press], "Database Connection":self.db_connections[press]}
        elif press=="05":
            result_dict={"Press Number": press, "Broker Connection":self.broker_connections[press], "Database Connection":self.db_connections[press], "Missing data":miss_list, "Constant data check":const_data,"Moving average difference":mov_avg_dict, "Temperature change":temp_diff}
            result_dict_with_connections = {"Press Number": press, "Broker Connection":self.broker_connections[press], "Database Connection":self.db_connections[press]}
        else:
            lube_peak=self.count_lube_peak(press)[0]
            result_dict={"Press Number": press, "Broker Connection":self.broker_connections[press], "Database Connection":self.db_connections[press], "Missing data":miss_list, "Constant data check":const_data,"Moving average difference":mov_avg_dict, "Temperature change":temp_diff, 
                    "Number of peaks":lube_peak}
            result_dict_with_connections = {"Press Number": press, "Broker Connection":self.broker_connections[press], "Database Connection":self.db_connections[press]}
        return result_dict, result_dict_with_connections


##############################################
######### Assign Warnings to PLC #############
##############################################
    
    def PLC_warning_signal(self,alert_code):
        if alert_code != 0:
            error_val = 0
            for i in alert_code["Temperature change"]: # dictionary with the sensor as the key and the temperature it changes by, make it error_val 2 is greater than 10 otherwise do 1
                if abs(alert_code["Temperature change"][i])  >= 10:
                    error_val = self.critical_code
                else:
                    error_val = max(error_val, self.warning_code)
            for i in alert_code["Number of peaks"]:
                if i: #put correct condition
                    error_val = max(error_val, self.warning_code)
            for i in alert_code["Moving average difference"]: #dictionary with the sensor as key and a percentage as a change (30 is the threshold)
                if abs(alert_code["Moving average difference"][i]) > 50 : #put correct condition
                    error_val = self.critical_code
                else:
                    error_val = max(error_val,self.warning_code)
            
            return error_val


##############################################
####### Send Valid alerts to DB and PLC ######
##############################################
    
    def send_alerts_to_DB_and_PLC(self,alert_code,press):
        logging.info("\nExecuting send_alerts_to_DB_and_PLC()")
        alert_collection = self.alert_db[f"Press_{press}"]
        previous_alert = None
        try:
            # Save the alerts in database
            if len(alert_code)!=0:
                from time import sleep

                # global previous_alert
                # global date
                alert_code.update({"_id":self.start_of_the_alert}) # Date as main index to avoid duplicate key error

                try:     #edge case of no entries in the database
                    previous_alert = alert_collection.find().limit(1).sort("$natural", -1)[0]
                except: 
                    pass      
                

                #query documents with the same date delete old document and add it to a new dictionary if it don't exist and then add the new dictionary           
                if alert_code != None and previous_alert != None:
                    alert_without_time = alert_code
                    try:
                        del alert_without_time['_id']
                    except:
                        pass

                    previous_without_time = previous_alert
                    
                    try:
                        del previous_without_time['_id']
                    except:
                        pass
                
                
                
                # try: # if an alert already exists for this time
                #     if (previous_alert != None) and (alert_code['_id'] == previous_alert['_id']):
                        
                #         alert_collection.delete_one(previous_alert)

                #         print("Updating Alert for this hour!")            
                    
                #     if (previous_alert != None) and (alert_without_time == previous_without_time) and ((previous_alert['_id']).hour != current_hour): 
                    
                #         alert_collection.delete_one(previous_alert)
                    
                #         print("Same Alert!")
                
                # except:
                    
                #     pass
                

                alert_code.update({"_id":self.start_of_the_alert})
                print(alert_code)
                # alert_collection.insert_one(alert_code)

                # add to plc needs to be editted for new thresholds
                # if press == '05':
                #     init_broker().mqttc_21_and_05.publish("Press_05_alert", self.PLC_warning_signal(alert_code))
                # elif press == '11':
                #     init_broker().mqttc_11.publish("Press_11_alert", self.PLC_warning_signal(alert_code))
                # elif press == '21':
                #     init_broker().mqttc_21_and_05.publish("Press_21_alert", self.PLC_warning_signal(alert_code))
                # elif press == '24':
                #     init_broker().mqttc_24.publish("Press_24_alert", self.PLC_warning_signal(alert_code))

                print("Alerts for the entire hour added")
            else:
                # if press == '05':
                #     init_broker().mqttc_21_and_05.publish("Press_05_alert","No Alerts/Warnings")    
                # elif press == '11':
                #     init_broker().mqttc_11.publish("Press_11_alert","No Alerts/Warnings")    
                # elif press == '21':
                #     init_broker().mqttc_21_and_05.publish("Press_21_alert","No Alerts/Warnings")    
                # elif press == '24':    
                #     init_broker().mqttc_24.publish("Press_24_alert","No Alerts/Warnings")
                pass

        except Exception as e:
            print(e)

########################################################################################        
################################# Main function of this class ##########################
########################################################################################
    
    def main(self):
        logging.info(f"Time to Evaluate all Presses for {self.time_now}")
        # Check for PlC-->Broker-->DB connection for all presses
        logging.info("Executing check_PLC_MQTT_DB_connection")
        self.check_PLC_MQTT_DB_connection()
        init_broker().configure_broker()
    
        # press_chosen = str(input("Enter a working Press you want to generate alerts for:. i.e '05', '11', '21', '24': \n"))
        for press_chosen in self.overall_connections:
            
            print(f"Generating alerts for Press_{press_chosen}")
            '''
            if press_chosen == '11' and self.broker_connections['11']:
                print()
                init_broker().mqttc_11.loop_start()
            elif press_chosen == '24' and self.broker_connections['24']:
                init_broker().mqttc_24.loop_start()
            elif (press_chosen == '05' or press_chosen == '21') and (self.broker_connections['05'] or self.broker_connections['21']):
                init_broker().mqttc_21_and_05.loop_start()
            '''
            # if self.check_if_alert_duplicate(press_chosen):
            #     logging.warning("Document with the same time already exists!")
            #     continue
            try:
                logging.info("Executing self.overall_connection..")
                condition_1 =  self.overall_connections[press_chosen]
                condition_2 = self.is_press_really_running(press_chosen)
                if (condition_1 and condition_2):
                    logging.info("\n Press is connected and running..")
                    alert_code = self.get_result(press_chosen)[0]
                elif condition_1:
                    logging.info("\n Press is connected and but not running..")
                    alert_code = self.get_result(press_chosen)[1]
                    
                self.send_alerts_to_DB_and_PLC(alert_code, press_chosen)
                print(self.previous_threshold)
                global previous_threshold_global
                previous_threshold_global[press_chosen] = self.previous_threshold.get(press_chosen)
                print(f"Checking Threshold {previous_threshold_global}")
            except Exception as e:
                print(f"Unable to generate results for Press_{press_chosen} because of error: {e}")
                continue



# ============================================
#           Main 
# ============================================
logging.info("Program starts. Initializing global variables....")
threshold_sheet = "Alert_generation_and_interlocking/press_threshold_list.xlsx"
live_sensor_sheet = "Alert_generation_and_interlocking/live_sensors_list.xlsx"
ip_sheet = "Alert_generation_and_interlocking/IP_list.xlsx"
sensor_threshold_percent={"lub": 30,"vrms": 50, "arms": 50, "apk": 50, "crst": 50}   #change percent threshold according to need, update in moving avg function
mongo_IP = "128.121.34.13"

# Calculate the threshold per press and store it in global dictionary
previous_threshold_global = {press: None for press in generate_send_signal(threshold_sheet,live_sensor_sheet,ip_sheet, sensor_threshold_percent, mongo_IP).presses_and_IP}

while 1:
    print(previous_threshold_global)
    logging.info("Executing generate_send_signal..")
    generate_send_signal(threshold_sheet,
                         live_sensor_sheet,
                         ip_sheet,
                         sensor_threshold_percent, 
                         mongo_IP).main()
    

# print("Now printing my dictionary",previous_threshold)
# anytime moving avg function is ran it return two things, the actual moving average stuff and the threshold dictionary by component, in get results I can store the threshold dictionary to a global variable 
# everytime I run moving average, i just check if the job is the same as the previous job, if it is, instead of calling get_thresholds everytime I can just just call the global dict and index the component
# it will work even press 24 but remember there is a condition inside so put this functionality inside there too
