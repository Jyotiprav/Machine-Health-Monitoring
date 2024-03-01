import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
import openpyxl
import pymongo
import os
#===============================================================
#Summary
#Goes through each sensor in batches, for all sensors, retrieve n amount of data frames of the different dates. 
#once all dates are collected, loop through the list of data frames and make a plot with subplots of the different dates.
#Once the plot is made, save the plot into a folder of other pngs and then put the plot into the excel file

current_dir = os.getcwd()

def singlejobcomparison(anglestart, angleend, datestr, pressangle, batch_info_dict, press_db, jobdates, part, filename):
    print("Loading excel file\n")
    print("*** DO NOT OPEN EXCEL FILE WHILE LOADING ***")
    #Creates a new sheet in a new excel file
    wb = openpyxl.Workbook()
    #split dates into a list of indexes of different dates to plot
    dates = datestr.split(',')
    for i in range(0, len(dates)):
        dates[i] = int(dates[i])

    #looping through each batch
    for batch in batch_info_dict:
        #creating a sheet for the batch for comparison
        sh = wb.create_sheet("BATCH_" + str(batch) + " Comparison")
        #setting collection
        collection = press_db["BATCH_" + str(batch)]
        #enumerating sensor for position in excel file
        for number, sensor in enumerate(batch_info_dict[batch]):
            print(number)
            print(sensor)
            errorcountlist = []
            listofdf = []
            #looping through each date in the list of dates given
            for date in dates:
                #retrieves the date and splits the times into a list where it is turned into integer and sets startdate and enddate
                time = jobdates[date]
                runtime = time.split(',')
                for i in range(0, len(runtime)):
                    runtime[i] = int(runtime[i])

                startdate = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
                enddate = datetime(runtime[6],runtime[7],runtime[8],runtime[9],runtime[10],runtime[11])


                #setting projection and query based on if press has press angle and if it is measuring vrms or arms to filter out error values
                projection = {'_id':0, 'Date':1, sensor:1}
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
                try:
                    results = collection.find(QUERY,projection)
                    filtercount = collection.count_documents(QUERY)
                    df = pd.DataFrame(results).set_index("Date")
                    listofdf += [df]

                    #setting errorcount by comparing document count with limit on sensor in query and without
                    if pressangle == "":
                        QUERY = {"Date": {'$gte': startdate, '$lt':  enddate}}
                    else:
                        QUERY = { '$and' : [{"Date": {'$gte': startdate, '$lt':  enddate}}, {pressangle : {'$gte':anglestart, '$lte': angleend}}]}
                    nofiltercount = collection.count_documents(QUERY)
                    errorcount = nofiltercount - filtercount
                    errorcountlist += [errorcount]
                except:
                    print("No date in sensor")

            #creating fig and axs for the plot
            if len(listofdf) != 0:
                fig, axs = plt.subplots(1, len(listofdf), sharey = True, figsize = (len(listofdf) * 3, 3))
                if len(listofdf) != 1:
                    for i in range(len(listofdf)):
                        #iterate through every axs to plot dataframe in subplots
                        axs[i].plot(listofdf[i])
                        axs[i].set_xlabel('Date_Time')
                        axs[i].set_ylabel(sensor)
                        axs[i].set_title("Errors: " + str(errorcountlist[i]))
                else:
                    #code for just one dataframe so it will not raise an error
                    axs.plot(listofdf[0])
                    axs.set_xlabel('Date_Time')
                    axs.set_ylabel(sensor)
                    axs.set_title("Errors: " + str(errorcountlist[0]))
                
                #setting xtick rotation, title
                fig.autofmt_xdate(rotation=45)

                fig.suptitle(part, fontsize=14, fontweight='bold')
                
                my_path = current_dir
                folder = "graphplotpngs"
                plot = sensor + ".png"

                #saving plot to put into excel file

                plt.tight_layout()  # Adjust spacing between subplots
                plt.subplots_adjust(wspace=0.3)
                plt.savefig(os.path.join(my_path, folder, plot), dpi = 120)
                plt.close()

                #load image

                img = openpyxl.drawing.image.Image(os.path.join(my_path, folder, plot))
                #add image and save
                sh.add_image(img, "A" + str((number * 19) + 1))
                wb.save(filename = filename)
    wb.close()

            
