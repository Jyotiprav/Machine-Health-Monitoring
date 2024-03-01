import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from datetime import timedelta
import os
import pymongo 
current_dir = os.getcwd()
def determine_outlier_thresholds_iqr(dataframe, col_name,th1,th3):
    quartile1 = dataframe[col_name].quantile(th1)
    quartile3 = dataframe[col_name].quantile(th3)
    iqr = quartile3 - quartile1
    upper_limit = quartile3 + 1.5 * iqr
    lower_limit = quartile1 - 1.5 * iqr
    return lower_limit, upper_limit
def check_outliers_iqr(dataframe, col_name,th1,th3):
    lower_limit, upper_limit = determine_outlier_thresholds_iqr(dataframe, col_name,th1,th3)
    if dataframe[(dataframe[col_name] > upper_limit) | (dataframe[col_name] < lower_limit)].any(axis=None):
        return True
    else: 
        return False

def replace_with_thresholds_iqr(dataframe,cols,replace=False,th1=0.0, th3=0.87):
    print("In filter")
    data = []
    for col_name in cols:
        if col_name != '_id':
            outliers_ = check_outliers_iqr(dataframe,col_name,th1,th3)
            count = None
            lower_limit, upper_limit = determine_outlier_thresholds_iqr(dataframe, col_name, th1, th3)
            if outliers_:
                count = dataframe[(dataframe[col_name] > upper_limit) | (dataframe[col_name] < lower_limit)][col_name].count()
                
                if replace: 
                    if lower_limit < 0:
                        # We don't want to replace with negative values, right!
                        dataframe.loc[(dataframe[col_name] > upper_limit), col_name] = upper_limit
                    else:
                        dataframe.loc[(dataframe[col_name] < lower_limit), col_name] = lower_limit
                        dataframe.loc[(dataframe[col_name] > upper_limit), col_name] = upper_limit
               
            outliers_status = check_outliers_iqr(dataframe, col_name,th1,th3)
    return dataframe
# anglestart, angleend, sensor, startdate, enddate, "Press_Angle", batch_info_dict, press_db
def plotrolling(anglestart, angleend, sensor,startdatestr,enddatestr, pressangle, batch_info_dict, press_db):
    startdatelist = startdatestr.split(',')
    enddatelist = enddatestr.split(',')

    #turning all str in list to int
    for i in range(0, len(startdatelist)):
        startdatelist[i] = int(startdatelist[i])
    for i in range(0, len(enddatelist)):
        enddatelist[i] = int(enddatelist[i])
    startdate   = datetime(startdatelist[0],startdatelist[1],startdatelist[2],startdatelist[3],startdatelist[4],startdatelist[5])
    enddate     = datetime(enddatelist[0],enddatelist[1],enddatelist[2],enddatelist[3],enddatelist[4],enddatelist[5])

    # startdate=datetime.now()-timedelta(hours=12)
    # enddate=datetime.now()
    
    for key in batch_info_dict:
        if sensor in batch_info_dict[key]:
            sbatch = "BATCH_" + str(key)

    collection = press_db[sbatch]
    if pressangle == "":
        projection = {'_id':0, 'Date':1, sensor:1}
    else:
        projection = {'_id':0, 'Date':1, pressangle : 1, sensor:1}
    #====================================================================================
    #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
    if pressangle == "":
        if "Vrms" in sensor:
            QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3}}]}
        elif "Arms" in sensor or "Apk" in sensor or "Apeak" in sensor:
            QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3000}}]}
        else:
            QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}}]}
    else:
        if "Vrms" in sensor:
            QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3}}]}
        elif "Arms" in sensor or "Apk" in sensor or "Apeak" in sensor:
            QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3000}}]}
        else:
            QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte' : 10000}}]}
    results = collection.find(QUERY, projection)
    df = pd.DataFrame(results).set_index("Date")
    print(df.head(5))
    #====================
    #filtering downtimes
    if pressangle != "" and ("Vrms" in sensor or "Arms" in sensor):
        df['angle_diff'] = df[pressangle].diff()

        significant_changes = df['angle_diff'].abs() > 0.001

        filtered_df = df[significant_changes]
        filtered_df = filtered_df.drop(["angle_diff", pressangle], axis = 1)
        filtered_df[sensor].rolling(window = timedelta(hours = 1)).mean().plot(title = sensor + " Moving Average")
        plt.xlabel("Date_Time")
        plt.show()
    else:
        df[sensor].rolling(window = timedelta(hours = 1)).mean().plot(title = sensor + " Moving Average")
        plt.show()

#==============================================================================================================================================================================

def multiplotMA(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, startdatestr, enddatestr, pressangle, batch_info_dict, press_db):

    listofdf = []

    startdatelist = startdatestr.split(',')
    enddatelist = enddatestr.split(',')

    #turning all str in list to int for dates
    for i in range(0, len(startdatelist)):
        startdatelist[i] = int(startdatelist[i])
    for i in range(0, len(enddatelist)):
        enddatelist[i] = int(enddatelist[i])

    startdate = datetime(startdatelist[0],startdatelist[1],startdatelist[2],startdatelist[3],startdatelist[4],startdatelist[5])
    enddate = datetime(enddatelist[0],enddatelist[1],enddatelist[2],enddatelist[3],enddatelist[4],enddatelist[5])

    #filters out all sensors in the batch dictionary that are not one of the 4 sensors plotted

    batch_dict = {}
    for key in batch_info_dict:
        for field in batch_info_dict[key]:
            if field == sensor1 or field == sensor2 or field == sensor3 or field == sensor4:
                if key in batch_dict:
                    batch_dict[key] += [field]
                else:
                    batch_dict[key] = [field]

    #looping through every batch in batch dict to ensure all 4 corners can be plotted even if in different batches

    for sbatch in batch_dict:
        collection = press_db["BATCH_" + str(sbatch)]
        if pressangle == "":
            projection = {'_id':0, 'Date':1, **{feature:1 for feature in batch_dict[sbatch]}}
        else:
            projection = {'_id':0, 'Date':1, pressangle : 1, **{feature:1 for feature in batch_dict[sbatch]}}
        #====================================================================================
        #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
        if pressangle == "":
            if "Vrms" in sensor1:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3} for sensor in batch_dict[sbatch]}]}
            elif "Arms" in sensor1 or "Apk" in sensor1 or "Apeak" in sensor1:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3000} for sensor in batch_dict[sbatch]}]}
            else:
                QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000} for sensor in batch_dict[sbatch]}]}
        else:
            if "Vrms" in sensor1:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3} for sensor in batch_dict[sbatch]}]}
            elif "Arms" in sensor1 or "Apk" in sensor1 or "Apeak" in sensor1:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3000} for sensor in batch_dict[sbatch]}]}
            else:
                QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte' : 10000} for sensor in batch_dict[sbatch]}]}
        results = collection.find(QUERY, projection)
        df = pd.DataFrame(results).set_index("Date")
        #====================
        #filtering downtimes
        if pressangle != "" and ("Vrms" in sensor1 or "Arms" in sensor1):
            df['angle_diff'] = df[pressangle].diff()

            significant_changes = df['angle_diff'].abs() > 0.001

            filtered_df = df[significant_changes]
            filtered_df = filtered_df.drop(["angle_diff", pressangle], axis = 1)
            listofdf += [filtered_df]
        else:
            listofdf += [df]
    
    #add all batch df together
    df = pd.concat(listofdf, ignore_index = False)
    df = df.sort_index()
    df[sensor1].rolling(window = timedelta(hours = 1)).mean().plot(title = "Four Corner Moving Average")
    df[sensor2].rolling(window = timedelta(hours = 1)).mean().plot()
    df[sensor3].rolling(window = timedelta(hours = 1)).mean().plot()
    df[sensor4].rolling(window = timedelta(hours = 1)).mean().plot()
    plt.xlabel("Date_Time")
    plt.legend()
    plt.show()
#=======================================================================================================================================
def movingsinglejob(anglestart, angleend, sensor,datestr, pressangle, batch_info_dict,press_db,jobdates,part):   # UPDATED : Oct 2
    
    #list of dataframes from diff dates
    lodf = []
    #split dates into a list of indexes of different dates to plot
    dates = datestr.split(',')
    #getting batch for the sensor
    for key in batch_info_dict:
            if sensor in batch_info_dict[key]:
                sbatch = "BATCH_" + str(key)
    
    collection = press_db[sbatch]
    #setting projection
    if pressangle == "":
        projection = {'_id':0, 'Date':1, sensor:1}
    else:
        projection = {'_id':0, 'Date':1, pressangle : 1, sensor:1}

    #turning all str in list to int
    for i in range(0, len(dates)):
        dates[i] = int(dates[i])
    #retreiving date from dataframe of times the job ran
    for job in dates:
        time = jobdates[job]
        #splitting the date
        runtime = time.split(',')
        #changing it all from str to int to be put into datetime format
        for i in range(0, len(runtime)):
            runtime[i] = int(runtime[i])

        startdate = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
        enddate = datetime(runtime[6],runtime[7],runtime[8],runtime[9],runtime[10],runtime[11])
        #====================================================================================
        #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
        if pressangle == "":
            if "Vrms" in sensor:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3}}]}
            elif "Arms" in sensor or "Apk" in sensor or "Apeak" in sensor:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3000}}]}
            else:
                QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000}}]}
        else:
            if "Vrms" in sensor:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3}}]}
            elif "Arms" in sensor or "Apk" in sensor or "Apeak" in sensor:
                QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3000}}]}
            else:
                QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte' : 10000}}]}
        results = collection.find(QUERY, projection)
        df = pd.DataFrame(results).set_index("Date")
        #====================
        #filtering downtimes
        if pressangle != "" and ("Vrms" in sensor or "Arms" in sensor or "Apk" in sensor or "Crst" in sensor):
            df['angle_diff'] = df[pressangle].diff()
            significant_changes = df['angle_diff'].abs() > 0.001

            filtered_df = df[significant_changes]
            filtered_df = filtered_df.drop(["angle_diff", pressangle], axis = 1)
            # filtered_df = replace_with_thresholds_iqr(filtered_df, filtered_df.columns,replace=True)  # UPDATED: No diiference in filtered moving average
            filtered_df = filtered_df[sensor].rolling(window = timedelta(hours = 1),center = True).mean()                
            lodf += [filtered_df]  # lodf is a list where items are values of different time ranges as series
            print("Line 244 in MAPlot.py Average of moving average ",lodf[-1].mean())  # UPDATED: for average of moving average
        #========================= if no press angle just add the moving average df to the list
        else:
            print("line 247")
            df = df[sensor].rolling(window = timedelta(hours = 1)).mean()
            lodf += [df]
    #return lodf[0]
    #create a plot
    fig, axs = plt.subplots(1, len(lodf), sharey = True, figsize = (len(lodf) * 3, 3))
    #loop through all axes and plot all dataframes
    #for loop for multiple dataframes and multiple subplots
    #print(len(lodf))
    if len(lodf) != 1:
        for i in range(len(lodf)):
            axs[i].plot(lodf[i])
            xfmt = mdates.DateFormatter('%m-%d %H:%M')
            axs[i].xaxis.set_major_formatter(xfmt)
            axs[i].set_xlabel('Date_Time')
            axs[i].set_ylabel(sensor)
    else:
        #code for just one dataframe so it will not raise an error
        axs.plot(lodf[0])
        xfmt = mdates.DateFormatter('%m-%d %H:%M')
        axs.xaxis.set_major_formatter(xfmt)
        axs.set_xlabel('Date_Time')
        axs.set_ylabel(sensor)
    
    fig.autofmt_xdate(rotation=45)

    fig.suptitle(part, fontsize=14, fontweight='bold')

    # my_path = current_dir
    # folder="p24_9_25"
    # plot=sensor+".png"

    plt.tight_layout()  # Adjust spacing between subplots
    plt.subplots_adjust(wspace=0.3)
    #plt.savefig(os.path.join(my_path,folder,plot), dpi = 300)
    #plt.close(fig)
    # del fig
    plt.show()
    
#===================================================================================================================================================================
def movingmultijob(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, datestr, pressangle, batch_info_dict, press_db, jobdates, part):

    listofjobdf = []

    dates = datestr.split(',')

    #filters out all sensors in the batch dictionary that are not one of the 4 sensors plotted

    for i in range(0, len(dates)):
        dates[i] = int(dates[i])

    batch_dict = {}
    for key in batch_info_dict:
        for field in batch_info_dict[key]:
            if field == sensor1 or field == sensor2 or field == sensor3 or field == sensor4:
                if key in batch_dict:
                    batch_dict[key] += [field]
                else:
                    batch_dict[key] = [field]

    #looping through every batch in batch dict to ensure all 4 corners can be plotted even if in different batches
    for job in dates:
        listofdf = []
        time = jobdates[job]
        runtime = time.split(',')
        for i in range(0, len(runtime)):
            runtime[i] = int(runtime[i])

        startdate = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
        enddate = datetime(runtime[6],runtime[7],runtime[8],runtime[9],runtime[10],runtime[11])
        #loops through batches in case the sensors in the four corners are in different batches
        for sbatch in batch_dict:
            collection = press_db["BATCH_" + str(sbatch)]
            if pressangle == "":
                projection = {'_id':0, 'Date':1, **{feature:1 for feature in batch_dict[sbatch]}}
            else:
                projection = {'_id':0, 'Date':1, pressangle : 1, **{feature:1 for feature in batch_dict[sbatch]}}
                # projection = {'_id':0, 'Date':1, **{feature:1 for feature in batch_dict[sbatch]}}
            #====================================================================================
            #query with date, press angle if press has press angle and filter out high error values so it doesnt effect the moving average
            if pressangle == "":
                if "Vrms" in sensor1:
                    QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3} for sensor in batch_dict[sbatch]}]}
                elif "Arms" in sensor1 or "Apk" in sensor1 or "Apeak" in sensor1:
                    QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte': 3000} for sensor in batch_dict[sbatch]}]}
                else:
                    QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {sensor : {'$lte' : 10000} for sensor in batch_dict[sbatch]}]}
            else:
                if "Vrms" in sensor1:
                    QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3} for sensor in batch_dict[sbatch]}]}
                elif "Arms" in sensor1 or "Apk" in sensor1 or "Apeak" in sensor1:
                    QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte': 3000} for sensor in batch_dict[sbatch]}]}
                else:
                    QUERY = {'$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}, {sensor : {'$lte' : 10000} for sensor in batch_dict[sbatch]}]}
            results = collection.find(QUERY, projection)
            df = pd.DataFrame(results).set_index("Date")
            #====================
            #filtering downtimes
            if pressangle != "" and ("Vrms" in sensor1 or "Arms" in sensor1):
                df['angle_diff'] = df[pressangle].diff()

                significant_changes = df['angle_diff'].abs() > 0.001

                filtered_df = df[significant_changes]
                filtered_df = filtered_df.drop(["angle_diff", pressangle], axis = 1)
                listofdf += [filtered_df]
            else:
                listofdf += [df]
        #add all batch df together
        df = pd.concat(listofdf, ignore_index = False)
        #sort index to prevent concat date errors
        df = df.sort_index()
        df1 = df.rolling(window = timedelta(hours = 1)).mean()
        listofjobdf += [df1]
    fig, axs = plt.subplots(1, len(listofjobdf), sharey = True, figsize = (len(listofjobdf) * 3, 3))
    #loop through all axes and plot all dataframes
    #for loop for multiple dataframes and multiple subplots
    title = sensor1.split("_")
    title = title[:-1]
    if title == "Vrms":
        title = "Velocity"
    elif title == "Arms":
        title = "Acceleration"
    elif title == "Pre" or title == "psi" or title == "Psi" or title == "PSI":
        title = "Pressure (PSI)"
    if len(listofjobdf) != 1:
        for i in range(len(listofjobdf)):
            axs[i].plot(listofjobdf[i])
            xfmt = mdates.DateFormatter('%m-%d %H:%M')
            axs[i].xaxis.set_major_formatter(xfmt)
            axs[i].set_xlabel('Date_Time')
            axs[i].set_ylabel(title)
    else:
        #code for just one dataframe so it will not raise an error
        axs.plot(listofjobdf[0])
        xfmt = mdates.DateFormatter('%m-%d %H:%M')
        axs.xaxis.set_major_formatter(xfmt)
        axs.set_xlabel('Date_Time')
        axs.set_ylabel(title)
    
    fig.autofmt_xdate(rotation=45)
    fig.suptitle(part, fontsize=14, fontweight='bold')
    plt.tight_layout()  # Adjust spacing between subplots
    plt.subplots_adjust(wspace=0.3)
    plt.legend(df.columns)
    plt.show()
