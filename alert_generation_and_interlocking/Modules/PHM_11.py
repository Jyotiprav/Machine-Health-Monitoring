import pymongo
from datetime import datetime, time, timedelta
import paho.mqtt.client as paho
import pandas as pd
class PHM_11:
    def __init__(self,press_number):
        self.start_time = ""
        self.end_time = ""
        self.db_IP_address = "128.121.34.13"
        self.myclient_global = pymongo.MongoClient(self.db_IP_address) 
        self.press_number = press_number       
        self.pressdb         = self.myclient_global[f'Press_{self.press_number}'] # Get press database
        self.collection_list = self.pressdb.list_collection_names()  # List of all collection names as strings
        # self.collection_list = ["BATCH_1"] # Currently publishing using beckhoff in one batch
        self.collection_list =[x for x in self.collection_list if "BATCH" in x]  # List of all collection names as strings
        self.alert_db =  self.myclient_global["Press_Alerts"]  
        self.alert_collection = self.alert_db[f"Press_{press_number}"]
        # Get the components using datapoints
        self.ma_thresholds = {}
        self.document_count_per_batch = {} # true means greater than zero and false means zero i.e. no document found 
        # Get total datapoints and components from database
        # Get current job numbers
        self.current_job = ""
        self.previous_job = ""
        self.lube_threshold_start_date=datetime(2024,2,21,7,0,0)
        self.lube_threshold_end_date=datetime(2024,2,21,9,0,0)
        
        # Get the job based thresholds at the beginning
        # self.ma_thresholds = self.set_thresholds_MA_per_datapoint_type() 
        
    def set_datapoints_and_components(self,start_time,end_time):
        # const_data_lst=check_constantdata(press_number) # Get the points that have constant data. No need to calculate average of constant data
        self.all_datapoint_per_collection_dict = {}
        self.component_list=[]
        self.all_datapoint_list = []
        for collection in self.collection_list:
            #Getting ONE document in specified date range.
            recent_document=self.pressdb[collection].find_one({"Date": {'$gte': start_time, '$lt':  end_time}}) 
            self.all_datapoint_per_collection_dict[collection]= [i for i in list(recent_document.keys()) 
                                                                if i!="_id"                                 #Avoid id key
                                                                and i!="Date"                               #Avoid date key
                                                                and i!="Ram_Adjust_Chain_On/Off"            #Avoid boolean values
                                                                and i!="job_id"                             #Avoid job string values
                                                                and i!="Press_angle"
                                                            ]
            self.all_datapoint_list+=self.all_datapoint_per_collection_dict[collection]
            
        for collection in self.all_datapoint_per_collection_dict:
            datapoint_list =self.all_datapoint_per_collection_dict[collection]
            for i in datapoint_list:
                # If its a combo sensor
                if "Vrms" in i or "Arms" in i or"Crest" in i or "Apk" in i or "Crst" in i or "Apeak" in i or "vrms" or "arms" or  "crst" or "apk" in i: 
                    code_list=i.split("_")
                    comp="_".join(code_list[:len(code_list)-1])
                    if comp not in self.component_list:
                        self.component_list.append(comp)
                else: # if its not a combo sensor
                    self.component_list.append(i)
        # print("Component found: ",len(self.component_list))
    def set_current_job_and_ma_thresholds(self):
        try:
            # Get job number using recent document
            # recent_document_from_db = self.pressdb["BATCH_1"].find().limit(1).sort("$natural", -1)
            # Get job number in specified time range
            # recent_document_from_db = self.pressdb["BATCH_1"].find({"Date": {'$gte': start_time, '$lt':  end_time}},{"job_id":1})
            # recent_job = recent_document_from_db[0]['job_id']
            recent_job = "MRT-68430748/49" # No job id for dec 14 data, manual input for job
            if recent_job == "":
                return "No Job Inputted"
            else :
                print(f"Press {self.press_number} : Currently running job = {recent_job}")
                self.current_job = recent_job
                if self.previous_job !=self.current_job:
                    self.previous_job = self.current_job
                    self.ma_thresholds = self.set_thresholds_MA_per_datapoint_type()
                else:
                    print("current job is same as previous job ",self.current_job)
        except Exception as e:
            print("Exception",e)
            
    def get_current_datapoints_per_type(self,datapoint_type:str):
        '''
            Description:
            Funtion to get certain datapoints. For e.g. only vrms or temp per collection/batch
        '''
        datapoint_per_type_and_collection_dict = {}
        for collection,datapoint_list in self.all_datapoint_per_collection_dict.items():
            datapoint_per_type_and_collection_dict[collection] = [i for i in datapoint_list if datapoint_type in i]
        return datapoint_per_type_and_collection_dict
    # Get historical datapoints
    def get_datapoints_per_type(self,datapoint_type,start_date,end_date):
        '''
            Description:
            Funtion to get certain datapoints in defined time range. 
            For e.g. only vrms or temp per collection/batch between start to end date.
        '''
        datapoint_per_type_and_collection_dict = {}
        datapoint_per_collection_dict = {}
        for collection in self.collection_list:
            recent_document=self.pressdb[collection].find_one({"Date": {'$gte': start_date, '$lt':  end_date}}) #Getting current keys
            datapoint_per_collection_dict[collection]= [i for i in list(recent_document.keys()) 
                                                                if i!="_id"                                 #Avoid id key
                                                                and i!="Date"                               #Avoid date key
                                                                and i!="Ram_Adjust_Chain_On/Off"            #Avoid boolean values
                                                                and i!="job_id"                             #Avoid job string values
                                                                and i!="Press_angle"
                                                            ]
        for collection,datapoint_list in datapoint_per_collection_dict.items():
            datapoint_per_type_and_collection_dict[collection] = [i for i in datapoint_list if datapoint_type in i]
        return datapoint_per_type_and_collection_dict
    #=========================================================
    # Document Count
    #=========================================================
    def get_document_count(self,start_time= None,end_time= None,):
        if start_time is None:
            start_time = self.start_time
            end_time =self.end_time
        query = {"Date": {"$gte": start_time, "$lt": end_time}}
        for i in self.collection_list:
            self.document_count_per_batch[i] = True if self.pressdb[i].count_documents(query) > 0 else False  
        return self.document_count_per_batch  
    #=========================================================
    # Get Missing Datapoints
    #=========================================================
    def get_missing_datapoints(self):
        print("\t#----------Getting Missing Datapoints----------#")
        missing_dp_list=[]
        sensor_sheet=pd.read_excel((live_sensors_list_path), sheet_name= f"Press_{self.press_number}",usecols="E,H")
        expected_datapoints = list(sensor_sheet["Tag Name"])
        if len(self.all_datapoint_list) > len(expected_datapoints): 
            for i in self.all_datapoint_list:
                if i not in expected_datapoints:
                    miss_list.append(i)
        elif len(self.all_datapoint_list) < len(expected_datapoints):
            for i in expected_datapoints:
                if i not in self.all_datapoint_list:
                    missing_dp_list.append(i)
        print("\t#----------COMPLETED : Missing Datapoints----------#")
        return missing_dp_list
    
    #=========================================================
    # Send Alert code 2 to PLC
    #=========================================================
    def send_critical_alert_to_PLC(self,broker_ip_address):
        try:
            broker=broker_ip_address #PLC
            port=1883
            def on_publish(client,userdata,result):                  #create function for callback
                pass
            client1= paho.Client("Alert Code Publisher")             #create client object
            client1.on_publish = on_publish                          #assign function to callback
            client1.connect(broker,port)                             #establish connection
            ret= client1.publish("alert_code","2")                   #publish
            return "send_critical_alert_to_PLC(): Code 2 sent to PLC"
        except Exception as E:
            print("Exception in send_critical_alert_to_PLC",E)
    #=========================================================
    # Detect constant data/error codes
    #=========================================================
    def get_constantdata(self):
        print("\n\t#----------Getting Constant Datapoints----------#")
        # For constant data 
        constantData_list = [] 
        for collection,datapoint_list in self.all_datapoint_per_collection_dict.items():
            #print("\n",collection,"\n",self.datawithdate[collection])
            projection = {'_id':0, **{datapoint:1 for datapoint in datapoint_list}}# Projection per batch
            # print("\nProjection:", projection)
            query = {"Date":{"$gte":self.start_time,"$lt":self.end_time}}
            results= self.pressdb[collection].find(query,projection)
            df= pd.DataFrame(results)
            df_diff = df.diff().abs()
            sum_diff = df_diff.sum(axis=0)
            for x in datapoint_list:
                if  sum_diff[x] == 0 and "fdl" not in x:
                    constantData_list += [x]
        # print(f"Press {self.press_number} :Constant datapoint list = ",constantData_list)
        print("\t#----------COMPLETED: Getting Constant Datapoints----------#")
        return constantData_list
     
    
    #=========================================================
    # Detect constant data/error codes ends here.
    #=========================================================
    
    #=========================================================
    # Temperature rate of change
    #=========================================================
    def get_temperature_rate_of_change(self,STATIC_rate_of_change_degrees=5,STATIC_temp_threshold=105):
        '''
            Description : 
            Function to get the difference between start time 
            temperature and end time temperature.
        '''
        sensors_per_batch_dict = self.get_current_datapoints_per_type("Temp")
        temperature_alert_dict = {}
        for batch,datapoints in sensors_per_batch_dict.items():
            collection = self.pressdb[batch]
            projection = {'_id':0, 'Date':1, **{datapoint:1 for datapoint in datapoints}}# Projection per batch
            query = {"Date":{"$gte":self.start_time,"$lt":self.end_time}}
            results= collection.find(query,projection)
            df= pd.DataFrame(results).set_index("Date")
            # print(df.mean())
            temperature_difference = df.iloc[-1] - df.iloc[0] # Pandas series
            temperature_alert_dict.update({key:[round(value,2),df[key].iloc[-1]] 
                                           for key,value in temperature_difference.to_dict().items() 
                                           if value>=int(STATIC_rate_of_change_degrees) or df[key].iloc[-1] >= int(STATIC_temp_threshold)})
            for value in temperature_alert_dict.values():
                if value[1]>=STATIC_temp_threshold and value[0]>0:
                    # pass
                    status =self.send_critical_alert_to_PLC("128.121.21.11")
                    print(status)
        return(temperature_alert_dict)
    
    
            
    
    
    #=========================================================
    # Moving Average 
    #=========================================================    
    
        
    def get_threshold_per_dp(self,job_number):
        print("\n#----------Getting MA Thresholds Timestamps per datapoints----------#\n")
        
        thresholds = pd.read_excel(threshold_sheet_path,f"{self.current_job}").set_index("Datapoint")
        
        df = thresholds.loc[thresholds.index == "Cl_Vrms"]
        dict = df["Start date"].to_dict()
        # df["Start date"] = df["Start date"].astype(str)
        start_date = datetime.strptime(df["Start date"].to_string() , '%y,/%m,/%d,%H,%M,%S')
        # print(start_date)
        
        # ["Start_date"] = datetime.strptime(thresholds["Cl_Vrms"]["Start_date"], '%y,/%m,/%d,%H,%M,%S')
        # print(thresholds["Cl_Vrms"]["Start_date"])
            # start_and_end = [thresholds["Start date"][self.current_job], thresholds["End date"][self.current_job]]    
            # for date in start_and_end:
     
    def get_thresholds_timestamps(self):
        print("\n#----------Getting MA Thresholds Timestamps----------#\n")
        # try:
        thresholds = pd.read_excel(threshold_sheet_path,f"Press {self.press_number}").set_index("Jobs").to_dict()
        start_and_end = [thresholds["Start date"][self.current_job], thresholds["End date"][self.current_job]]    
        for date in start_and_end:
            #splitting the date
            runtime = date.split(',')
            #changing it all from str to int to be put into datetime format
            for i in range(0, len(runtime)):
                runtime[i] = int(runtime[i])
            if date == start_and_end[0]:
                job_start_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
            elif date == start_and_end[1]:
                job_end_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
        print("\nThreshold start:",job_start_date,"\nThreshold ends:",job_end_date) 
        return [job_start_date,job_end_date]  
        # except:
        #     print("get_thresholds_timestamps() : Can not read the thresholds.xlsx")
        #     return []
        
    
    def filter_downtime(self,df):
        df['angle_diff'] = df["Press_angle"].diff()
        significant_changes = df['angle_diff'].abs() > 0.001
        filtered_df = df[significant_changes]
        filtered_df = filtered_df.drop(["angle_diff", "Press_angle"], axis = 1)
        return filtered_df 
        
    def set_thresholds_MA_per_datapoint_type(self):
        thresholds_timestamps_list = self.get_thresholds_timestamps()
        if thresholds_timestamps_list!=[]:
            start_time=thresholds_timestamps_list[0]
            end_time=thresholds_timestamps_list[1]
            lube_threshold_dict ={}
            ma_window_size = timedelta(seconds=15)
            # Get the datapoints using componentlist
            print("\n#----------Getting MA Thresholds Values----------#\n")
            # Get and remove constant data if any
            # constant_datapoints_list = self.get_constantdata(start_time,end_time)
            constant_datapoints_list = ['Lubrication_Oil_Level_mm', 'Pit_level_mm', 'Lubrication_B_05_Pressure_psi', 'P_R_Pressure_psi', 'S_R_Pre_psi'] # For TESTING
            for item in [
                        ("Vrms",3.26),("Arms",3260),
                        ("Crst",3260),("Apk",3260),
                        ("Apeak",3260),
                        ("Pre",5000),
                        ("Psi",5000),("psi",5000),("Level",32760)
                        #  ("Temp",500), # Don't need temperature moving average since we are checking rate of change
                        ]:
                current_datapoints = self.get_datapoints_per_type(item[0],start_time,end_time)
                for collection,datapoint_list in current_datapoints.items():
                    datapoint_list = [item for item in datapoint_list if item not in constant_datapoints_list]
                    if len(datapoint_list)!=0:
                        print(f"get_thresholds_per_datapoint_type(): Total number of data in {collection} of type {item[0]} = {len(datapoint_list)}",)
                        projection = {"_id":0, "Date":1, "Press_angle":1, **{datapoint:1 for datapoint in datapoint_list}}
                        query = { '$and' : [{"Date": {'$gte': start_time, '$lt':  end_time}},
                                            {datapoint:{'$lt': item[1]} for datapoint in datapoint_list}
                                            ]}
                        try:
                            results = self.pressdb[collection].find(query, projection)      
                            df = pd.DataFrame(results).set_index("Date")
                            df = self.filter_downtime(df)
                            ma_dict = {datapoint : round((df[datapoint].rolling(window = ma_window_size,center = True).mean()).mean(),4) for datapoint in datapoint_list}
                            lube_threshold_dict.update(ma_dict) 
                        except: # This is to continue if all the numbers under datapoint has constant data or error codes.
                            print(f"Exception caught in get_thresholds_per_datapoint_type() for datapoint {df.columns}")
                # Sorting threshold dictionary high to low value
                
            lube_threshold_dict={key:value for key,value in sorted(lube_threshold_dict.items(), key=lambda item: item[1],reverse=True)}
            print("\n#----------Keys for which moving average was not calculated. ----------#\n")
            print(f"\nLength of All data points = {len(self.all_datapoint_list)} \nLength of All thresholds keys = {len(lube_threshold_dict)}")
            
            if len(self.all_datapoint_list) > len(lube_threshold_dict):
                print("\nKeys not present : ")
                for key in self.all_datapoint_list:
                    if key not in lube_threshold_dict and key not in constant_datapoints_list and "Temp" not in key:
                        print(key,end=",")
            print("\n#----------COMPLETED: Getting MA Thresholds Values ----------#\n")
            return lube_threshold_dict
                
    def get_current_MA_per_datapoint_type(self,start_time,end_time):
        lube_threshold_dict ={}
        ma_window_size = timedelta(seconds=15)
        # Get the datapoints using componentlist
        print("\n#----------Getting Current MA Values----------#\n")
        # Get and remove constant data if any
        # constant_datapoints_list = self.get_constantdata(start_time,end_time)
        constant_datapoints_list = ['Lubrication_Oil_Level_mm', 'Pit_level_mm', 'Lubrication_B_05_Pressure_psi', 'P_R_Pressure_psi', 'S_R_Pre_psi'] # For TESTING
        for item in [
                    ("Vrms",3.26),("Arms",3260),
                    ("Crst",3260),("Apk",3260),
                    ("Apeak",3260),
                    ("Pre",5000),
                    ("Psi",5000),("psi",5000),("Level",32760)
                    #  ("Temp",500), # Don't need temperature moving average since we are checking rate of change
                    ]:
            current_datapoints = self.get_datapoints_per_type(item[0],start_time,end_time)
            for collection,datapoint_list in current_datapoints.items():
                datapoint_list = [item for item in datapoint_list if item not in constant_datapoints_list]
                if len(datapoint_list)!=0:
                    print(f"get_thresholds_per_datapoint_type(): Total number of data in {collection} of type {item[0]} = {len(datapoint_list)}",)
                    projection = {"_id":0, "Date":1, "Press_angle":1, **{datapoint:1 for datapoint in datapoint_list}}
                    query = { '$and' : [{"Date": {'$gte': start_time, '$lt':  end_time}},
                                        {datapoint:{'$lt': item[1]} for datapoint in datapoint_list}
                                        ]}
                    try:
                        results = self.pressdb[collection].find(query, projection)      
                        df = pd.DataFrame(results).set_index("Date")
                        df = self.filter_downtime(df)
                        ma_dict = {datapoint : round((df[datapoint].rolling(window = ma_window_size,center = True).mean()).mean(),4) for datapoint in datapoint_list}
                        lube_threshold_dict.update(ma_dict) 
                    except: # This is to continue if all the numbers under datapoint has constant data or error codes.
                        print(f"Exception caught in get_thresholds_per_datapoint_type() for datapoint {df.columns}")
            # Sorting threshold dictionary high to low value
            
        lube_threshold_dict={key:value for key,value in sorted(lube_threshold_dict.items(), key=lambda item: item[1],reverse=True)}
        print("\n#----------Keys for which moving average was not calculated. ----------#\n")
        print(f"\nLength of All data points = {len(self.all_datapoint_list)} \nLength of All thresholds keys = {len(lube_threshold_dict)}")
        
        if len(self.all_datapoint_list) > len(lube_threshold_dict):
            print("\nKeys not present : ")
            for key in self.all_datapoint_list:
                if key not in lube_threshold_dict and key not in constant_datapoints_list and "Temp" not in key:
                    print(key,end=",")
        print("\n#----------COMPLETED: Getting Current MA  Values ----------#\n")
        return lube_threshold_dict
        
    def compare_current_vs_thresholds(self):
        ma_alerts_dict = {}
        ma_prcnt = 30
        print("\t#----------Comparing MA Thresholds Timestamps with current timestamps----------#")
        ma_current = self.get_current_MA_per_datapoint_type(self.start_time,self.end_time)
        for datapoint,threshold in self.ma_thresholds.items():
            if threshold !=0:
                roc_prcnt = ((ma_current[datapoint]-threshold)/threshold)*100
                if roc_prcnt> ma_prcnt:
                    ma_alerts_dict[datapoint]={"Rate of change":round(roc_prcnt,2),
                                       "Threshold mean value":round(threshold,3), 
                                       "Recent mean values":round(ma_current[datapoint],3)}
        return(ma_alerts_dict)
    #=========================================================
    # Lubrication pressure drop and peaks calculation
    #=========================================================
    
    def get_lubrication_rate_of_change(self):
        '''
         Description:
         Unit to calculate the rate of change using moving average and last best pressure threshold
         threshold start and end dates will be the day we setup the system
        '''
        # Get the list of sensors. For press 11 it can be hard coded. Else have lub condition
        # list_of_lub_sensors = self.get_current_datapoints_per_type("lub") # condition for press 24
        lube_alert_dict ={}
        sensors_per_batch_dict = {'BATCH_2': ['Cr_LBlock1_Pre','Lub_PreFltr_Pre', 'Lub_PostFltr_Pre'], 
                                  'BATCH_1': ['Lubrication_B_05_Pressure_psi']}# Only works for press 11
        lube_threshold_dict = self.get_lubrication_pressure_thresholds(self.lube_threshold_start_date,self.lube_threshold_end_date)
        lube_current_values = self.get_lubrication_pressure_thresholds(self.start_time,self.end_time)
        print("Avg lubrication threshold :",lube_threshold_dict)
        print("Current lubrication values :",lube_current_values)
        for datapoint,threshold in lube_threshold_dict.items():
            if threshold !=0:
                roc_prcnt = abs((lube_current_values[datapoint]-threshold)/threshold)*100
                if roc_prcnt> 30:
                    lube_alert_dict[datapoint] = {"Rate of change":round(roc_prcnt,2),
                                       "Threshold mean value":round(threshold,2), 
                                       "Recent mean values":round(lube_current_values[datapoint],2)}
                    
        return(lube_alert_dict)
    def get_lubrication_pressure_thresholds(self,start_time,end_time):
        sensors_per_batch_dict = {'BATCH_2': ['Cr_LBlock1_Pre','Lub_PreFltr_Pre', 'Lub_PostFltr_Pre'], 
                                  'BATCH_1': ['Lubrication_B_05_Pressure_psi']}
        lube_threshold_dict = {}
        for batch,datapoints in sensors_per_batch_dict.items():
            collection = self.pressdb[batch]
            projection = {'_id':0, 'Date':1, **{datapoint:1 for datapoint in datapoints}}# Projection per batch
            query = {"Date":{"$gte":start_time,"$lt":end_time}}
            results= collection.find(query,projection)
            df= pd.DataFrame(results).set_index("Date")
            lube_threshold_dict.update(df.mean().to_dict()) # Average of lube value ONLY
        
        return(lube_threshold_dict)
    
    #=========================================================
    # Generate Alert Code
    #=========================================================
    def generate_alert_code(self):
        print(f"\n#----------GENERATING COMPLETE ALERTS DOCUMENT FOR {self.start_time}----------#\n")
        self.set_current_job_and_ma_thresholds()
        temperature_alert_dict  = self.get_temperature_rate_of_change()
        missing_dp_alert_list   = self.get_missing_datapoints()
        constant_dp_list = self.get_constantdata()
        lube_alert_dict = self.get_lubrication_rate_of_change()
        ma_avg_dict = self.compare_current_vs_thresholds()
        for dp in lube_alert_dict:
            if dp in ma_avg_dict:
                del ma_avg_dict[dp]
        complete_alert_code = {
            "_id":self.start_time,
            "Press Number": self.press_number,
            "Job Number":self.current_job,
            "Temperature change":temperature_alert_dict,
            "Broker Connection":True,
            "Database Connection":True,
            "Missing data":missing_dp_alert_list,
            "Constant data check":constant_dp_list,
            "Moving average difference":ma_avg_dict,
            "Lube psi average difference":lube_alert_dict,
            "Number of peaks":{"Cr_LBlock1_Pre":{"Average peaks":{"340-90":34,"90-180":78,"180-270":78,"270-340":90},
                                               "Average pressure":{"340-90":34,"90-180":78,"180-270":78,"270-340":90}
                            }}
        }
        self.alert_collection.insert_one(complete_alert_code)
        print("Alert saved in database!!")


# threshold_sheet_path =  r"/home/pressshop_stats/Predictive_Maintenance/github repo/Alert_generation_and_interlocking/press_threshold_list.xlsx"
# live_sensors_list_path = r"/home/pressshop_stats/Predictive_Maintenance/github repo/Alert_generation_and_interlocking/live_sensors_list.xlsx"
# press_11_object = PHM_11(press_number="11")

# for i in range(13,20):   
#     start_time  = datetime(2024,2,5,i-1,0,0)        #For temp testing
#     end_time    = datetime(2024,2,5,i,0,0)          #For temp testing
#     press_11_object.start_time  =start_time
#     press_11_object.end_time    =end_time
#     press_11_object.set_datapoints_and_components(start_time,end_time)
#     press_11_object.generate_alert_code()
    
    
# For bypass option testing

# import time 
# press_11_object = PHM_11("11")
# for i in range(7,12):   
#     start_time  = datetime(2023,12,14,i,0,0)        #For temp testing
#     end_time    = datetime(2023,12,14,i+1,0,0)      #For temp testing
#     press_11_object.start_time  =start_time
#     press_11_object.end_time    =end_time
#     press_11_object.set_datapoints_and_components(start_time,end_time)
#     print(press_11_object.get_temperature_rate_of_change("11"))
#     press_11_object.generate_alert_code()
#     time.sleep(10)

# for i in range(6,13): 
#     start_time      = datetime(2024,2,1,i-4,0,0) 
#     end_time        = datetime(2024,2,1,i,0,0)
#     press_11_object = PHM_11("11",start_time,end_time)
#     press_11_object.get_lubrication_rate_of_change(datetime(2024,1,15,6,0,0),datetime(2024,1,15,12,0,0))
#     print("=================================")
