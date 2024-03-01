import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
import os

def plotdata(anglestart, angleend, sensor, startdatestr, enddatestr, pressangle, batch_info_dict, press_db):
    #split start date end date
    startdatelist = startdatestr.split(',')
    enddatelist = enddatestr.split(',')

    #turning all str in list to int
    for i in range(0, len(startdatelist)):
        startdatelist[i] = int(startdatelist[i])
    for i in range(0, len(enddatelist)):
        enddatelist[i] = int(enddatelist[i])
    #set startdate and enddate
    startdate = datetime(startdatelist[0],startdatelist[1],startdatelist[2],startdatelist[3],startdatelist[4],startdatelist[5])
    enddate = datetime(enddatelist[0],enddatelist[1],enddatelist[2],enddatelist[3],enddatelist[4],enddatelist[5])
    #set batch by seeing if sensor is in a batch
    for key in batch_info_dict:
        if sensor in batch_info_dict[key]:
            sbatch = "BATCH_" + str(key)

    collection = press_db[sbatch]
    projection = {'_id':0, 'Date':1, sensor:1}
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
    results = collection.find(QUERY,projection)
    filtercount = collection.count_documents(QUERY)
    df = pd.DataFrame(results).set_index("Date")
    
    #===========================================================
    #Calculating error count by not filtering out errors and finding the difference in document count
    if pressangle == "":
        QUERY = {"Date": {'$gte': startdate, '$lt':  enddate}}
    else:
        QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lte':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}]}
    nofiltercount = collection.count_documents(QUERY)
    errorcount = nofiltercount - filtercount
    #plotting the data
 
    plt.figure(figsize = (10,6))
    plt.plot(df[sensor], color = 'tab:blue')
    plt.xlabel('Date_Time')
    plt.ylabel(sensor)
    plt.xticks(rotation = 45)
    plt.title(str(sensor) + ' Errors: ' + str(errorcount))
    current_dir = os.getcwd()
    my_path = current_dir
    # folder="p24_check"
    # plot=sensor+".png"
    # plt.savefig(os.path.join(my_path,folder,plot), dpi = 300)
    plt.show()
#=================================================================================================================================================================
def plotjobdata(anglestart, angleend, sensor, datestr, pressangle, batch_info_dict, press_db, jobdates, part):
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
        if pressangle != "" and ("Vrms" in sensor or "Arms" in sensor or "Apk" in sensor or "Crst" in sensor): # UPDATED
            df['angle_diff'] = df[pressangle].diff()
            significant_changes = df['angle_diff'].abs() > 0.001
            filtered_df = df[significant_changes]
            filtered_df = filtered_df.drop(["angle_diff", pressangle], axis = 1)
            lodf += [filtered_df]
        #========================= if no press angle just add the moving average df to the list
        else:
            lodf += [df]
    #create a plot
    fig, axs = plt.subplots(1, len(lodf), sharey = True, figsize = (len(lodf) * 3, 3))
    #loop through all axes and plot all dataframes
    #for loop for multiple dataframes and multiple subplots
    print(len(lodf))
    if len(lodf) != 1:
        for i in range(len(lodf)):
            axs[i].plot(lodf[i])
            axs[i].set_xlabel('Date_Time')
            axs[i].set_ylabel(sensor)
    else:
        #code for just one dataframe so it will not raise an error
        axs.plot(lodf[0])
        # axs.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1, 7)))
        # axs.xaxis.set_minor_locator(mdates.MonthLocator())
        # axs.grid(True)
        axs.set_xlabel('Date_Time')
        axs.set_ylabel(sensor)
    
    fig.autofmt_xdate(rotation=45)

    fig.suptitle(part, fontsize=14, fontweight='bold')

    plt.tight_layout()  # Adjust spacing between subplots
    plt.subplots_adjust(wspace=0.3)
    plt.show()