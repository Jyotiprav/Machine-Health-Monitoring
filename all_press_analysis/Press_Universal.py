import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
from datetime import datetime
import os
import xlwings as xw
from openpyxl import Workbook

# pd.set_option("display.max_rows", None)
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
def press_universal_describe(press,production_times,angle_start,angle_end,column_list):

    current_directory = os.getcwd()

    myclient_global = pymongo.MongoClient(host = "128.121.34.13",connect = True)
    press_db = myclient_global[press]

    df = pd.read_excel(f"{current_directory}\live_sensors_list.xlsx",sheet_name = press)
    tag_name = df['Tag Name']
    tag_name = list(tag_name)

    batch_info_dict = {} #empty dict

    if column_list == []:
        for batch in press_db.list_collection_names():
            col = press_db[batch]
            results = col.find().limit(1).sort("_id",-1) # getting the first entry in db
            recent_doc = results[0] #getting the first entry in db
            # print(list(recent_doc.keys()))
            batch_key_list = list(recent_doc.keys())
            column_list += batch_key_list

    for column in column_list:
        for batch in press_db.list_collection_names():
            collection = batch
            col = press_db[collection]
            results = col.find().limit(1).sort("_id",-1) # getting the first entry in db
            recent_doc = results[0] #getting the first entry in db
            # print(list(recent_doc.keys()))
            batch_key_list = list(recent_doc.keys())
            if column in batch_key_list:
                if batch not in batch_info_dict:
                    batch_info_dict[batch] = [column]
                else:
                    batch_info_dict[batch].append(column)

    quantiles = pd.DataFrame()

    for key in batch_info_dict.keys():
        # batch = "BATCH_" + str(key)
        batch = key
        collection= press_db[batch]

        fields = batch_info_dict[key]

        projection = {}
        projection['_id'] = 0
        projection['Date'] = 1
        if press == "Press_11":
            projection['Press_angle'] = 1
        elif press == "Press_24":
            projection['Press_Angle'] = 1
        for field in fields:
            projection[field] = 1

        def filtered(df,minutes=40*1*60):
            '''
            By default function filters out every minute when press isn't running (40 datapoints per second times 60 seconds per minute)
            This can be changed by specifying minute variable

            Example:
            filtered(df) would filter data for every minute press isn't running
            filtred(df,5) would filter data for every 5 minutes press isn't running
            '''
            
            # Split the dataframe into smaller chunks
            df_chunks = [df[i:i+minutes] for i in range(0, len(df), minutes)]
            # Filter out chunks with zero standard deviation in 'Press_Angle'
            if press == "Press_11":
                filtered_chunks = [chunk for chunk in df_chunks if np.std(chunk['Press_angle']) != 0]
            elif press == "Press_24":
                filtered_chunks = [chunk for chunk in df_chunks if np.std(chunk['Press_Angle']) != 0]

            filtered_df = pd.concat(filtered_chunks, ignore_index=True)
            # 2. Remove outliers
            filtered_df = replace_with_thresholds_iqr(filtered_df, filtered_df.columns,replace=True)
            # df= filtered_df
            return filtered_df
        quantile = pd.DataFrame()
        for production_time in production_times:
            start= production_time[0]
            end = production_time[1]
            if press == "Press_11":
                QUERY = {'$and':[{"Date": {'$gte': start, '$lt':  end}},{"Press_angle" : {'$gte':angle_start, '$lte': angle_end}}]}
            elif press == "Press_21" or press=="Press_05":
                QUERY = {"Date": {'$gte': start, '$lt':  end}}
            elif press == "Press_24":
                QUERY = {'$and':[{"Date": {'$gte': start, '$lt':  end}},{"Press_Angle" : {'$gte':angle_start, '$lte': angle_end}}]}
            results = collection.find(QUERY,projection)
            df = pd.DataFrame(results).set_index('Date')
            if press in ["Press_11","Press_24"]:
                filtered_df = filtered(df)
            else:
                filtered_df = df
            if quantile.empty:
                quantile = filtered_df.describe() ## for interactive.py purposes only
            else:
                quantile += filtered_df.describe() ## for interactive.py purposes only
        quantiles = pd.concat([quantiles,quantile],axis=1)
        # print(quantile.T / len(production_times))
    return (quantiles.T / len(production_times)).dropna()


    # print(batch_info_dict)
# print(press_universal(24,[[datetime(2023,7,11,14,0,0),datetime(2023,7,11,14,10,0)],[datetime(2023,7,18,6,0,0),datetime(2023,7,18,6,5,0)]],0,45,[]))
# print(press_universal(24,[[datetime(2023,7,9,12,45,0),datetime(2023,7,10,1,45,0)]],0,45,[]))

def press_universal(press_number,production_times,angle_start,angle_end,column_list):
    print(press_number)
    if press_number==5:
        press="Press_05"
    else:
        press = "Press_" + str(press_number)
    print(press)
    current_directory = os.getcwd()

    myclient_global = pymongo.MongoClient(host = "128.121.34.13",connect = True)
    press_db = myclient_global[press]

    df = pd.read_excel(f"{current_directory}\live_sensors_list.xlsx",sheet_name = press)
    tag_name = df['Tag Name']
    tag_name = list(tag_name)

    batch_info_dict = {} #empty dict

    if column_list == []:
        for batch in press_db.list_collection_names():
            col = press_db[batch]
            results = col.find().limit(1).sort("_id",-1) # getting the first entry in db
            recent_doc = results[0] #getting the first entry in db
            # print(list(recent_doc.keys()))
            batch_key_list = list(recent_doc.keys())
            column_list += batch_key_list

    for column in column_list:
        for batch in press_db.list_collection_names():
            collection = batch
            col = press_db[collection]
            results = col.find().limit(1).sort("_id",-1) # getting the first entry in db
            recent_doc = results[0] #getting the first entry in db
            # print(list(recent_doc.keys()))
            batch_key_list = list(recent_doc.keys())
            if column in batch_key_list:
                if batch not in batch_info_dict:
                    batch_info_dict[batch] = [column]
                else:
                    batch_info_dict[batch].append(column)

    quantiles = pd.DataFrame()

    for key in batch_info_dict.keys():
        # batch = "BATCH_" + str(key)
        batch = key
        collection= press_db[batch]

        fields = batch_info_dict[key]

        projection = {}
        projection['_id'] = 0
        projection['Date'] = 1
        if press_number == 11:
            projection['Press_angle'] = 1
        elif press_number == 24:
            projection['Press_Angle'] = 1
        for field in fields:
            projection[field] = 1

        def filtered(df,minutes=40*1*60):
            '''
            By default function filters out every minute when press isn't running (40 datapoints per second times 60 seconds per minute)
            This can be changed by specifying minute variable

            Example:
            filtered(df) would filter data for every minute press isn't running
            filtred(df,5) would filter data for every 5 minutes press isn't running
            '''
            
            # Split the dataframe into smaller chunks
            df_chunks = [df[i:i+minutes] for i in range(0, len(df), minutes)]

            # Filter out chunks with zero standard deviation in 'Press_Angle'
            if press_number == 11:
                filtered_chunks = [chunk for chunk in df_chunks if np.std(chunk['Press_angle']) != 0]
            elif press_number == 24:
                filtered_chunks = [chunk for chunk in df_chunks if np.std(chunk['Press_Angle']) != 0]

            filtered_df = pd.concat(filtered_chunks, ignore_index=True)

            return filtered_df

        quantile = pd.DataFrame()

        for production_time in production_times:
            start= production_time[0]
            end = production_time[1]
            if press_number == 11:
                QUERY = {'$and':[{"Date": {'$gte': start, '$lt':  end}},{"Press_angle" : {'$gte':angle_start, '$lte': angle_end}}]}
            elif press_number == 21 or press_number==5:
                QUERY = {"Date": {'$gte': start, '$lt':  end}}
            elif press_number == 24:
                QUERY = {'$and':[{"Date": {'$gte': start, '$lt':  end}},{"Press_Angle" : {'$gte':angle_start, '$lte': angle_end}}]}
            results = collection.find(QUERY,projection)
            df = pd.DataFrame(results).set_index('Date')
            if press_number in [11,24]:
                filtered_df = filtered(df)
            else:
                filtered_df = df
            if quantile.empty:
                # quantile = filtered_df.describe() ## for interactive.py purposes only
                quantile = filtered_df
            else:
                # quantile += filtered_df.describe() ## for interactive.py purposes only
                quantile += filtered_df

            # print(quantile)

        quantiles = pd.concat([quantiles,quantile],axis=1)

        # print(quantiles)

        # print(quantile.T / len(production_times))
    quantiles = quantiles.drop(['_id'],axis=1)

    return (quantiles / len(production_times))

def excel_plotter(file_name,df_list):
    workbook = Workbook()
    file_name += ".xlsx"
    workbook.save(file_name)
    sheet = xw.Book(file_name).sheets[0]
    starting_row = 0

    # df_list = [df1,df2,df3,df4]
    letters = ['B','L','V','AF']
    for i in range(len(df_list)):
        for j in range(len(df_list[i].columns)):
            excel_top = 6 + 30*j + starting_row

            # min_range = float("inf")
            min_range = 100000000000
            for k in range(len(df_list)):
                df_i_min = min(df_list[k].iloc[:,j].to_list())
                min_range = min(min_range,df_i_min)
            min_range *= 0.9

            max_range = 0
            for k in range(len(df_list)):
                df_i_max = max(df_list[k].iloc[:,j].to_list())
                max_range = max(max_range,df_i_max)
            max_range *= 1.1
            # min_range = max(0,min(df1.iloc[:,j].to_list() + df2.iloc[:,j].to_list() + df3.iloc[:,j].to_list() + df4.iloc[:,j].to_list()) * 0.9)
            # max_range = max(df1.iloc[:,j].to_list() + df2.iloc[:,j].to_list() + df3.iloc[:,j].to_list() + df4.iloc[:,j].to_list() ) * 1.1
            # if df4.columns[j].endswith(('Vrms','Arms','Apeak','Crest','Temp')):
            #     min_range, max_range = plot_range(df4.columns[j])
            fig = plt.figure()
            plt.plot(df_list[i].iloc[:,j],color='powderblue' if i < len(df_list) / 2 else 'gold')
            plt.ylabel(df_list[i].columns[j])
            if i < len(df_list):
                plt.xlabel('Df 1')
            else:
                plt.xlabel('Df 2')
            plt.xticks(rotation=90)
            # if df4.columns[j].endswith(('Vrms','Arms','Apeak','Crest','Temp')):
            # plt.ylim(min_range,max_range)
            # plt.ylim(max_range,min_range)
            # sheet.pictures.add(fig, name=f'{df_list[i].columns[j]}-{j}', update=True,left=sheet.range(f'{letters[i]}{excel_top}').left, top=sheet.range(f'{letters[i]}{excel_top}').top)
            sheet.pictures.add(fig, name=f'df{i}-{df_list[i].columns[j]}', update=True,left=sheet.range(f'{letters[i]}{excel_top}').left, top=sheet.range(f'{letters[i]}{excel_top}').top)
        
    # return excel_top + 30

# print(press_universal(24,[[datetime(2023,7,9,12,45,0),datetime(2023,7,10,1,45,0)]],0,45,[]))

if __name__ == '__main__':
    df1 = press_universal(24,[[datetime(2023,7,9,18,45,0),datetime(2023,7,10,0,0,0)]],0,45,[])
    df2 = press_universal(24,[[datetime(2023,7,9,18,45,0),datetime(2023,7,10,0,0,0)]],0,45,[])
    print(df1)
    print(df2)
    excel_plotter('Plot test',[df1,df2])