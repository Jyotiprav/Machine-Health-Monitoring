#imports
#========================================================================================
print("Importing modules... \n")

import pymongo
import os
import pandas as pd
import time 
import sys
sys.path.insert(0, '../Alert_generation_and_interlocking/Modules/')
from PHM_11 import PHM_11
#===================================================================================
#Initializing variables
press = ""
column_list = []
batch_info_dict = {}
analyzing = True
task = 0
current_dir = os.getcwd()
# Put onedrive link
# current_dir = r"C:\Users\JSharma\OneDrive - Martinrea Inc\General - Telemetry - Analysis and findings"
#====================================================================================
#Connect to mongo client
myclient_global = pymongo.MongoClient(host = '128.121.34.13', Connect = True)
#=====================================================================================
#Ask for press and set batches
while press not in ["Press_11", "Press_21", "Press_24","Press_05"]:
    press = str(input("Enter a valid Press (Press_11, Press_21, Press_24, Press_05): "))
    print()
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
#=======================================================================================

#Asking for Task
while analyzing == True:

#********UNCOMMENT AFTER WEEKLY ANALYSIS********
    
    print('''Tasks:
1. Find Job Thresholds
2. Single Job Comparison Over Time
3. 2 Job Comparison
4. Plot moving averages (single sensor or 4 corners/ job based comparison)
5. Plot sensor (date / job based comparison)
6. Error Checker (NaN Values, Constant Data)
7. Run weekly analysis
8. check p24
9. Get temp thresholds
(TYPE "quit" TO QUIT)\n''')
    task =  str(input("What would you like to do? (Enter the number): "))
    print()

#======================================================================================
#Job Thresholds
    if task == "1":
        print("Job Thresholds Selected\n")
        from Task1 import *
        task1(press)
#======================================================================================
#Single Job comparison
    elif task == "2":
        print("Single Job Comparison over time selected\n")
        from sensorjobcomparison import *
        #import job details to print tonnage spm counterbal and production sheet to give times of job runs
        prodsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_production_details")
        jobsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_job_details")
        print("Job Details (Tonnage, Counterbalance, SPM)")
        print(jobsheet.to_string(index = False))
        print()
        (("Job Selection"))
        #print out the job list
        joblist = prodsheet.columns.to_list()
        #print out all jobs and the correlated number
        for job in enumerate(joblist):
            print(job)
        print()
        jobchosen = int(input("Please enter the number correlated to what job you would like to choose: "))
        print()
        #set the dates based on the job chosen and print the jobs
        jobdates = prodsheet.iloc[:, jobchosen]
        print(jobdates)
        print()
        #getting part name for plot title
        part = joblist[jobchosen]
        #getting str of what dates are suppose to be selected
        dates = str(input('Enter the numbers of the dates you would like to plot (e.g "0,3,4,8"): '))
        print()
        #getting file name
        filename = str(input("What would you like to name the file (e.g comparison.xlsx)? "))
        print()
        if press == "Press_11":
            anglestart = int(input("What is the start angle: "))
            angleend = int(input("What is the end angle: "))
            print()
            singlejobcomparison(anglestart, angleend, dates, "Press_angle", batch_info_dict, press_db, jobdates, part, filename)
        elif press == "Press_24":
            anglestart = int(input("What is the start angle: "))
            angleend = int(input("What is the end angle: "))
            print()
            singlejobcomparison(anglestart, angleend, dates, "Press_Angle", batch_info_dict, press_db, jobdates, part, filename)
        elif press=="Press_05":
            singlejobcomparison(0, 360, dates, "", batch_info_dict, press_db, jobdates, part, filename)
        elif press=="Press_21":
            singlejobcomparison(0, 360, dates, "", batch_info_dict, press_db, jobdates, part, filename)
        

#======================================================================================
#2 Job Comparison
    elif task == "3":
        print("2 Job Comparison selected\n")
        from Task3 import *
        task3()
#======================================================================================
#Moving Averages
    elif task == "4":
        print("Plot Moving Average(s) selected\n")
        from MAplot import *
        #------------------------------------
        #ask for single sensor rolling average or four corners
        print('''Tasks:
            1. Single Sensor Moving Average Plot
            2. Four Corner Moving Average Plot\n''')
        
        sensorplottype = int(input("Select a task (Enter the number): "))
        print()
        if sensorplottype == 1:
            #ASKING FOR JOB BASED
            sensor = str(input("Please enter a sensor name: "))
            print()
            jobbased = str(input("Do you want to plot job based moving averages? (yes, no): "))
            # print()
            #jobbased="yes"
            #if not job based, ask for date and then run
            if jobbased.lower() == "no":
                try:
                    startdate = str(input('Please enter a start date time in format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
                    enddate = str(input('Please enter an end date in date time format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
                    print()
                    if press == "Press_11":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        plotrolling(anglestart, angleend, sensor, startdate, enddate, "Press_angle", batch_Pinfo_dict, press_db)
                    elif press == "Press_24":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        plotrolling(anglestart, angleend, sensor, startdate, enddate, "Press_Angle", batch_info_dict, press_db)
                    elif press == "Press_21" or press=="Press_05":
                        plotrolling(0, 360, sensor, startdate, enddate, "", batch_info_dict, press_db)
                except Exception as e:
                    print(f"Error message: There was in error in plotting, Please ensure the sensor name is correct/valid at the date and ensure the date format is valid.\n Exception {e}")
            #==========================================================================================================================
            #Job based, show jobs and then show dates for the job
            elif jobbased.lower() == "yes":
                try:
                    #read the sheets with the production dates and job details including tonnage and spm
                    prodsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_production_details")
                    jobsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_job_details")
                    print("Job Details (Tonnage, Counterbalance, SPM)")
                    print(jobsheet.to_string(index = False))
                    print()
                    (("Job Selection"))
                    #print out the job list
                    joblist = prodsheet.columns.to_list()
                    #print out all jobs and the correlated number
                    for job in enumerate(joblist):
                        print(job)
                    print()
                    jobchosen = int(input("Please enter the number correlated to what job you would like to choose: "))
                    #jobchosen=7
                    print()
                    #set the dates based on the job chosen and print the jobs
                    jobdates = prodsheet.iloc[:, jobchosen]
                    print(jobdates)
                    print()
                    #getting part name for plot title
                    part = joblist[jobchosen]
                    #getting str of what dates are suppose to be selected
                    dates = str(input('Enter the numbers of the dates you would like to plot (e.g "0,3,4,8"): '))
                    #dates="0,5,9,15,18"
                    print()
                    if press == "Press_11":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        movingsinglejob(anglestart, angleend, sensor, dates, "Press_angle", batch_info_dict, press_db, jobdates, part)
                    elif press == "Press_24":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        movingsinglejob(anglestart, angleend, sensor, dates, "Press_Angle", batch_info_dict, press_db, jobdates, part)
                    elif press == "Press_21" or press=="Press_05":
                        movingsinglejob(0, 360, sensor, dates, "", batch_info_dict, press_db, jobdates, part)
                except Exception as e:
                     print(f"Error message: There was in error in plotting, Please ensure the sensor name is correct/valid at the date and ensure the date format is valid.\n Exception {e}")
                
        if sensorplottype == 2:
            #filtering based off of press angle field
            sensor1 = str(input("Please enter first sensor: "))
            print()
            sensor2 = str(input("Please enter second sensor: "))
            print()
            sensor3 = str(input("Please enter third sensor: "))
            print()
            sensor4 = str(input("Please enter fourth sensor: "))
            print()
            jobbased = str(input("Do you want to plot job based moving averages? (yes, no): "))
            print()  
            #if not job based, ask for date and then run
            #jobbased="yes"
            if jobbased.lower() == "no":
                try:
                    startdate = str(input('Please enter a start date time in format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
                    enddate = str(input('Please enter an end date in date time format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
                    print()
                    if press == "Press_11":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        multiplotMA(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, startdate, enddate, "Press_angle", batch_info_dict, press_db)
                    elif press == "Press_24":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        multiplotMA(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, startdate, enddate, "Press_Angle", batch_info_dict, press_db)
                    elif press == "Press_21" or press=="Press_05":
                        multiplotMA(0, 360, sensor1, sensor2, sensor3, sensor4, startdate, enddate, "", batch_info_dict, press_db)
                except Exception as e:
                     print(f"Error message: There was in error in plotting, Please ensure the sensor name is correct/valid at the date and ensure the date format is valid.\n Exception {e}")
            elif jobbased.lower() == "yes":
                try:
                    prodsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_production_details")
                    jobsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_job_details")
                    print("Job Details (Tonnage, Counterbalance, SPM)")
                    print(jobsheet.to_string(index = False))
                    print()
                    (("Job Selection"))
                    joblist = prodsheet.columns.to_list()
                    for job in enumerate(joblist):
                        print(job)
                    print()
                    jobchosen = int(input("Please enter the number correlated to what job you would like to choose: "))
                    print()
                    jobdates = prodsheet.iloc[:, jobchosen]
                    print(jobdates)
                    print()
                    part = joblist[jobchosen]
                    dates = str(input('Enter the numbers of the dates you would like to plot (e.g "0,3,4,8"): '))
                    print()
                    if press == "Press_11":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        movingmultijob(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, dates, "Press_angle", batch_info_dict, press_db, jobdates, part)
                    elif press == "Press_24":
                        anglestart = int(input("What is the start angle: "))
                        angleend = int(input("What is the end angle: "))
                        print()
                        movingmultijob(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, dates, "Press_Angle", batch_info_dict, press_db, jobdates, part) #change to anglestart, angleend after weekly analysis
                    elif press == "Press_21" or press=="Press_05":
                        movingmultijob(0, 360, sensor1, sensor2, sensor3, sensor4, dates, "", batch_info_dict, press_db, jobdates, part)
                except Exception as e:
                     print(f"Error message: There was in error in plotting, Please ensure the sensor name is correct/valid at the date and ensure the date format is valid.\n Exception {e}")
        cont = str(input("Do you want to keep using? (yes, no): "))
        if cont == "no":
            break

#======================================================================================
#Plot sensor
    elif task == "5":
        print("Plotting Sensor selected")
        print()
        from sensorplot import *
        #get sensor that needs to be plotted
        sensor = str(input("Please enter a sensor name: "))
        print()
        jobbased = str(input("Do you want to plot sensor based on different job runs? (yes, no): "))
        print()
        #if not job based ask for date and press angle then run
        if jobbased.lower() == "no":
            try:
                startdate = str(input('Please enter a start date time in format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
                enddate = str(input('Please enter an end date in date time format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
                print()
                if press == "Press_11":
                    anglestart = int(input("What is the start angle: "))
                    angleend = int(input("What is the end angle: "))
                    print()
                    plotdata(anglestart, angleend, sensor, startdate, enddate, "Press_angle", batch_info_dict, press_db)
                elif press == "Press_24":

                    anglestart = int(input("What is the start angle: "))
                    angleend = int(input("What is the end angle: "))
                    print()
                    plotdata(anglestart, angleend, sensor, startdate, enddate, "Press_Angle", batch_info_dict, press_db)
                elif press == "Press_21" or press=="Press_05":
                    plotdata(0, 360, sensor,startdate, enddate,"", batch_info_dict, press_db) #UPDATED
            except Exception as e:
                 print(f"Error message: There was in error in plotting, Please ensure the sensor name is correct/valid at the date and ensure the date format is valid.\n Exception {e}")
        #if job based, load excel files with prod. dates and job details
        elif jobbased.lower() == "yes":
            try:
                prodsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name = press + "_production_details")
                jobsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name = press + "_job_details")
                print("Job Details (Tonnage, Counterbalance, SPM)")
                print(jobsheet.to_string(index = False))
                print()
                (("Job Selection"))
                joblist = prodsheet.columns.to_list()
                for job in enumerate(joblist):
                    print(job)
                print()
                #ask for job 
                jobchosen = int(input("Please enter the number correlated to what job you would like to choose: "))
                print()
                #index the dates based on job chosen
                jobdates = prodsheet.iloc[:, jobchosen]
                print(jobdates)
                print()
                #set part to the name of part for plot title
                part = joblist[jobchosen]
                #ask for which dates should be plotted
                dates = str(input('Enter the numbers of the dates you would like to plot (e.g "0,3,4,8"): '))
                print()
                if press == "Press_11":
                    anglestart = int(input("What is the start angle: "))
                    angleend = int(input("What is the end angle: "))
                    print()
                    plotjobdata(anglestart, angleend, sensor, dates, "Press_angle", batch_info_dict, press_db, jobdates, part)
                elif press == "Press_24":
                    anglestart = int(input("What is the start angle: "))
                    angleend = int(input("What is the end angle: "))
                    print()
                    plotjobdata(anglestart, angleend, sensor, dates, "Press_Angle", batch_info_dict, press_db, jobdates, part)
                elif press == "Press_21" or press=="Press_05":
                    plotjobdata(0, 360, sensor, dates, "", batch_info_dict, press_db, jobdates, part)
            except Exception as e:
                 print(f"Error message: There was in error in plotting, Please ensure the sensor name is correct/valid at the date and ensure the date format is valid.\n Exception {e}")
    
        cont = str(input("Do you want to keep using? (yes, no): "))
        if cont == "no":
            break
#=====================================================================================
#Error Check
    elif task == "6":
        print("Error check selected\n")
        print("Checking...\n")
        if press == "Press_24":
            from press24constantdatacheck import *
            p24constantdata()
            print()
        elif press == "Press_21":
            from press21constantdatacheck import*
            p21constantdata()
            print()
        elif press == "Press_11":
            from press11constantdatacheck import *
            p11constantdata()
            print()
        cont = str(input("Do you want to keep using? (yes, no): "))
        if cont == "no":
            break

#====================================================================================
    elif task =="7":
        from MAplot import *
        current_dir=os.getcwd()
        sensorsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press, usecols="E")
        vlist=sensorsheet.values.tolist()
        fr_mavg=[]
        extr=[]
        lst=[]
        for ele in vlist:
            for sens in ele:
                if ("FL" in sens) or ("FR" in sens) or ("RR" in sens) or ("RL" in sens): 
                    fr_mavg.append(sens)
                elif ("Arms" in sens) or ("Apk" in sens) or ("Apeak" in sens) or ("Temp" in sens) or ("Crst" in sens) or ("Crest" in sens):
                    extr.append(sens)
                else:
                    lst.append(sens)
        if press=="Press_24":
            lst.remove('Press_angle')
        prodsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name = press + "_production_details")
        jobsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name = press + "_job_details")
        print("Job Details (Tonnage, Counterbalance, SPM)")
        print(jobsheet.to_string(index = False))
        print()
        (("Job Selection"))
        joblist = prodsheet.columns.to_list()
        for job in enumerate(joblist):
            print(job)
        print()
        #ask for job 
        jobchosen = int(input("Please enter the number correlated to what job you would like to choose: "))
        print()
        #index the dates based on job chosen
        jobdates = prodsheet.iloc[:, jobchosen]
        print(jobdates)
        print()
        #set part to the name of part for plot title
        part = joblist[jobchosen]
        #ask for which dates should be plotted
        dates =str(input('Enter the numbers of the dates you would like to plot (e.g "0,3,4,8"): '))
        print()
        #got to MAplot and change the folder per press per week
        start=time.time()
        lst_error=[]
        lst=['M_Mtr_Vrms ', 'FwhlShaftBshg_In_Vrms', 'BGr_Bshg_Vrms', 'LCBal_Resvr_Pre_PSI', 'RCBal_Resvr_Pre', 'RadMtr_Vrms', 'Fdr_Pnch_Rler_Pnmatic_Pre', 'Fdr_UpDwn_Pnmatic_Pre', 'Fdr_Guide_Pre', 'Stnr_Pmp_HydPre', 'Stnr_Pmp_Vrms', 
            'Stnr_HydResvr_OilLvl', 'Stnr_HydMtrVrms', 'Stnr_MandrelArmRoller_FwdPre', 'Stnr_MandrelArmRoller_RevPre', 'uncolr_Mandrel_ret_Pre', 'uncolr_Mandrel_Exp_Pre', 'uncolr_Mandrel_JgFwd_Pre', 'uncolr_Mandrel_JgRev_Pre', 'uncolr_ClutchnBrake_Pre', 'DieClamp1_Pre', 'DieClamp2_Pre', 'QDC_UnClamp_Pre', 'QDC_Clamp_Pre', 'QDC_Lift_Pre', 'MainHPU_Psressure_Pre', 'BlstrHPU_OilLvl', 'Pnmatic_Driving_Pre', 'CrwnFr_LBlockPre', 
            'Pit_Hyd_Prefilter_Real_PSI', 'Pit_HydPre_Postfilter_Real_PSI', 'Pit_HydOverload_Pre_Real_PSI', 'Pit_Pmp_Vrms', 'Pit_LubeLevel', 'Pit_Mtr_Vrms', 'PitFlood', 'XferHyd_Pre']
        if press=="Press_21":
            for sensor in lst:
                try:
                    movingsinglejob(0, 360, sensor, dates, "", batch_info_dict, press_db, jobdates, part)
                except Exception as e:
                    print("Error found in sensor", sensor, "for date numbers", dates,"Exception ",e)
                    continue
        elif press=="Press_24":
            for sensor in lst:
                try:
                    movingsinglejob(0, 360, sensor, dates, "", batch_info_dict, press_db, jobdates, part)
                except Exception as e:
                    lst_error.append(sensor)
                    #print("Error found in sensor", sensor, "for date numbers", dates)
                    continue
        end=time.time()
        print()
        print((end-start)/60)
        print()
        print(lst_error)
        print()
        cont = str(input("Do you want to keep using? (yes, no): "))
        if cont == "no":
            break
    elif task=='8':
        current_dir=os.getcwd()
        sensorsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press, usecols="E")
        vlist=sensorsheet.values.tolist()
        fr_mavg=[]
        extr=[]
        lst=[]
        for ele in vlist:
            for sens in ele:
                if ("FL" in sens) or ("FR" in sens) or ("RR" in sens) or ("RL" in sens): 
                    fr_mavg.append(sens)
                elif ("Arms" in sens) or ("Apk" in sens) or ("Apeak" in sens) or ("Temp" in sens) or ("Crst" in sens) or ("Crest" in sens):
                    extr.append(sens)
                else:
                    lst.append(sens)
        print("Plotting Sensor selected")
        print()
        from sensorplot import *
        #get sensor that needs to be plotted
        for sensor in lst:
            print()
            #if not job based ask for date and press angle then run
            try:
                startdate = "2023,9,21,0,0,0"
                enddate = "2023,9,28,23,0,0"
                print()
                anglestart = 0
                angleend=360
                print()
                plotdata(anglestart, angleend, sensor, startdate, enddate, "Press_angle", batch_info_dict, press_db)
            except Exception as e:
                print("Error in sensor: ",sensor, "Exception ",e)
        cont = str(input("Do you want to keep using? (yes, no): "))
        if cont == "no":
            break

    # Task to get temperature base/thresholds
    elif task == "9":
        from datetime import datetime, time, timedelta
        threshold_dict ={}
        ma_window_size = timedelta(minutes=30)
        
        press_11_object = PHM_11(press_number="11")
        startdatestr = str(input('Please enter a start date time in format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
        enddatestr = str(input('Please enter an end date in date time format (e.g "2023,1,15,15,30,30", year/month/day/hour/minute/second): '))
        print()
        startdatelist = startdatestr.split(',')
        enddatelist = enddatestr.split(',')

        #turning all str in list to int
        for i in range(0, len(startdatelist)):
            startdatelist[i] = int(startdatelist[i])
        for i in range(0, len(enddatelist)):
            enddatelist[i] = int(enddatelist[i])
        startdate   = datetime(startdatelist[0],startdatelist[1],startdatelist[2],startdatelist[3],startdatelist[4],startdatelist[5])
        enddate     = datetime(enddatelist[0],enddatelist[1],enddatelist[2],enddatelist[3],enddatelist[4],enddatelist[5])

        press_11_object.start_time  =startdate
        press_11_object.end_time    =enddate
        # Get all datapoints for user entered range
        press_11_object.set_datapoints_and_components(startdate,enddate)
        # Get all temp sensors
        temp_sensors_per_batch_dict = press_11_object.get_current_datapoints_per_type("Temp")
        # constant_datapoints_list = press_11_object.get_constantdata()
        for collection,datapoint_list in temp_sensors_per_batch_dict.items():
            # datapoint_list = [item for item in datapoint_list if item not in constant_datapoints_list]
            if len(datapoint_list)!=0:
                print(f"get_thresholds_temp(): Total number of data in {collection} of type TEMP = {len(datapoint_list)}",)
                projection = {"_id":0, "Date":1, "Press_angle":1, **{datapoint:1 for datapoint in datapoint_list}}
                query = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}},
                            {datapoint:{'$lt': 5000} for datapoint in datapoint_list}
                                            ]}
                try:
                    results = press_11_object.pressdb[collection].find(query, projection)      
                    df_no_filter = pd.DataFrame(results).set_index("Date")
                    df = press_11_object.filter_downtime(df_no_filter)
                    print("Length of df befor and after downtime filter:",len(df),"/",len(df_no_filter))
                    for datapoint in datapoint_list:
                        print(datapoint,end=",")
                        print(df.describe()[datapoint]["mean"],end=",")
                        print(df.describe()[datapoint]["max"],end=",")
                        print(df.describe()[datapoint]["min"])
                        
                        
                    # ma_dict = {datapoint : round((df[datapoint].rolling(window = ma_window_size,center = True).mean()).mean(),4) for datapoint in datapoint_list}
                    # threshold_dict.update(ma_dict) 
                except: # This is to continue if all the numbers under datapoint has constant data or error codes.
                    print(f"Exception caught in get_thresholds_per_datapoint_type() for datapoint {df.columns}")
                # Sorting threshold dictionary high to low value
                
            # threshold_dict={key:value for key,value in sorted(threshold_dict.items(), key=lambda item: item[1],reverse=True)}
            # print("\n#----------Keys for which moving average was not calculated. ----------#\n")
            # # print(f"\nLength of All data points = {len(self.all_datapoint_list)} \nLength of All thresholds keys = {len(threshold_dict)}")
            
           
            print("\n#----------COMPLETED: Getting MA Thresholds Values ----------#\n")
            # print(threshold_dict)
                
        
        
        
#=====================================================================================
    elif task == "quit":
        break
#==========================
#invalid number

    else:
        print("Enter a valid number")
        print()