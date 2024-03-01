import json
import pickle
import pymongo
import numpy as np
import pandas as pd
import seaborn as sns
from tensorflow import keras
from datetime import datetime
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler, StandardScaler

def determine_outlier_thresholds_iqr(dataframe, col_name, th1=0.25, th3=0.75):
    quartile1 = dataframe[col_name].quantile(th1)
    quartile3 = dataframe[col_name].quantile(th3)
    iqr = quartile3 - quartile1
    upper_limit = quartile3 + 1.5 * iqr
    lower_limit = quartile1 - 1.5 * iqr
    return lower_limit, upper_limit
def check_outliers_iqr(dataframe, col_name):
    lower_limit, upper_limit = determine_outlier_thresholds_iqr(dataframe, col_name)
    if dataframe[(dataframe[col_name] > upper_limit) | (dataframe[col_name] < lower_limit)].any(axis=None):
        return True
    else: 
        return False

def replace_with_thresholds_iqr(dataframe,cols,replace=False,th1=0.25, th3=0.75):
    print("In filter")
    from tabulate import tabulate
    data = []
    for col_name in cols:
        if col_name != 'Outcome':
            outliers_ = check_outliers_iqr(dataframe,col_name)
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
               
            outliers_status = check_outliers_iqr(dataframe, col_name)
            data.append([outliers_, outliers_status, count, col_name, lower_limit, upper_limit ])
    return dataframe

class data_pre_processing():
      def __init__(self):
            self.myclient_global = pymongo.MongoClient(host = "128.121.34.11", connect = True)
            self.db_name="Press_24"
            self.db = self.myclient_global[self.db_name] 
            
      def set_values(self,start,end,n_steps,column_list):
            import pymongo
            self.start = start
            self.end   = end
            self.n_steps      = n_steps
            self.column_list = column_list
            self.batch_info_dict = {}
            self.myclient_global = pymongo.MongoClient(host = "128.121.34.11", connect = True)
            self.db_name="Press_24"
            self.db = self.myclient_global[self.db_name] 
            self.current_job = ""
            self.find_collection() # Finding the batches
      # ==========================================================
      def get_current_jobId(self):
            first_batch_db  = self.db['BATCH_1']
            doc             = first_batch_db.find().sort('$natural',-1).limit(1)
            self.current_job     = doc[0]['job_id']
            return self.current_job
            
      def find_collection(self):
            for column in self.column_list:
                  # print(column)
                  for batch in [1,2,3,4]:
                        try:
                              collection = "BATCH_"+str(batch)
                              db = self.db[collection]
                              results = db.find({'Date':{"$gte":self.start,"$lt":self.end}}).limit(1) #OLD KEYS
                              recent_doc = results[0]
                              # print(recent_doc)
                              if column in recent_doc.keys():
                                    if batch not in self.batch_info_dict:
                                          self.batch_info_dict[batch] = [column]
                                    else:
                                          self.batch_info_dict[batch].append(column)
                        except Exception as E:
                              pass
                              # print(f"find_collection exception(): {batch}",E)
                             
            
            print("Batch information :",self.batch_info_dict)
            # self.batch_info_dict[list(self.batch_info_dict.keys())[0]].append("Date")
            # print("Batch Infor Dict",self.batch_info_dict)
      # ========================================================== 
      def mongodb_to_X(self):
            list_of_dataframe=[]                                                                      
            df = pd.DataFrame(columns=self.column_list)   # Empty dataframe with column name                                   
            for batch in self.batch_info_dict:
                  collection = self.db["BATCH_"+str(batch)]                                                                           
                  projection = {"_id":0,"Date":1,"Press_Angle":1}       
                  try:                                  
                        for col in self.batch_info_dict[batch]:   
                              # ===============================
                              # Filter out any error code      
                              #================================                        
                              projection[col]=1                
                              if "vrms" in col:        
                                    QUERY = { '$and' : [{"Date": {'$gte': self.start, '$lt':  self.end}}, {col : {'$lte': 3}}]}     
                              elif "arms" in col:
                                    QUERY = { '$and' : [{"Date": {'$gte': self.start, '$lt':  self.end}}, {col : {'$lte': 3000}}]}    
                              else:
                                    QUERY = { "Date": {'$gte': self.start, '$lt':  self.end}}              
                              results = collection.find(QUERY,projection)   
                              df_1= pd.DataFrame(results)
                              # print("Number of documents in database before downtime filter:",len(df_1))
                              # ================================
                              # Filter the downtime
                              # ================================
                              df_1['angle_diff'] = df_1["Press_Angle"].diff()
                              significant_changes = df_1['angle_diff'].abs() > 0.001
                              filtered_df = df_1[significant_changes]
                              filtered_df = filtered_df.drop(["angle_diff", "Press_Angle"], axis = 1)
                              # print("Number of documents in database after downtime filter:",len(filtered_df))                
                              df[col]=filtered_df[col]      
                              # print(df.info())
                  except Exception as e:
                        print(e)  
            
            if len(df)!=0:
                  # ================================
                  # Data in different batch has different sample rate. 
                  # The following code is to fill the Null values
                  # in uneven column length dataframe.
                  # ================================
                  def f(x):
                        vals = x[~x.isnull()].values
                        vals = np.resize(vals, len(x))
                        return vals
    
                  df = df.apply(f, axis=0)
                  X = list()                                                              
                  end = 0                                                                                   
                  while end < len(df)-1:                                                                    
                        n_steps = self.n_steps                                                                             
                        new_end = end+n_steps                                                                      
                        if end>len(df)-1 or len(df[end:new_end])<n_steps:                                   
                              break
                        seq_x = df[end:new_end].to_numpy()
                        X.append(seq_x)
                        end = new_end
                  X= np.array(X)
                  print(f"--> Shape of the data: batch_size={X.shape[0]}, time_steps={X.shape[1]}, features={X.shape[2]}")
                  # scaler = MinMaxScaler()
                  # X_scaled = scaler.fit_transform(X.reshape(-1, X.shape[-1])).reshape(X.shape)
                  # print("shape of train after scaling ",X_scaled.shape)
                  return X,df  
            else:
                  print("Dataframe is empty returning empty lists")
                  # return [],[]
            # if len(df)!=0:                                 
            #       # df.set_index("Date",inplace=True)
            #       pass
            # else:
            #       print("Dataframe is empty.")            
            
            #  Include a data cleaning step here
            # 1. remove all data where press was not running
            # df['angle_diff'] = df["Press_Angle"].diff()
            # significant_changes = df['angle_diff'].abs() > 0.001
            # filtered_df = df[significant_changes]
            # filtered_df = filtered_df.drop(["angle_diff"], axis = 1)
            # print("removed DT")
            
            #  # 2. Remove outliers
            # filtered_df = replace_with_thresholds_iqr(filtered_df, filtered_df.columns,replace=True)
            # df= filtered_df
            # print("List of columns:",list(df.columns))             
            # Data preparation 1D to 3D(# of strokes, # of points, # of colums)                             
            
#=================================================================


      
      